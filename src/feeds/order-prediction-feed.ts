import {Calculate, retry, sleep} from '@fetching-korea/common-utils'
import {GoogleSpreadsheet} from 'google-spreadsheet'
import {parse} from 'json2csv'
import {isDate, isEmpty, isNil, isString, parseInt} from 'lodash'
import {DateTime} from 'luxon'

import sheetData from '../../fetching-sheet.json'

import {MySQL, S3Client} from '../utils'

import {iFeed} from './feed'

export class OrderPredictionFeed implements iFeed {
	async getTsvBufferWithRange(start: Date, end: Date, targetDocId = null, targetSheetId = null): Promise<Buffer> {
		return Buffer.from(await this.getTsv({start, end, targetDocId, targetSheetId}), 'utf-8')
	}

	async getTsvBuffer(): Promise<Buffer> {
		return Buffer.from(await this.getTsv({start: null, end: null}), 'utf-8')
	}

	async getTsv({start, end, targetDocId = '1jdeeoxYli6FnDWFxsWNAixwiWI8P1u0euqoNhE__OF4', targetSheetId}: { start: Date, end: Date, targetDocId?: string, targetSheetId?: string }): Promise<string> {
		const beforeShippingStatus = ['BEFORE_DEPOSIT', 'ORDER_AVAILABLE', 'ORDER_WAITING', 'PRE_ORDER_REQUIRED', 'ORDER_COMPLETE', 'ORDER_DELAY', 'ORDER_DELAY_IN_SHOP', 'PRODUCT_PREPARE']
		const overseasStatus = ['SHIPPING_START', 'IN_WAYPOINT_SHIPPING', 'WAYPOINT_ARRIVAL']
		const localStatus = ['DOMESTIC_CUSTOMS_CLEARANCE', 'CUSTOMS_CLEARANCE_DELAY', 'IN_DOMESTIC_SHIPPING', 'SHIPPING_COMPLETE', 'ORDER_CONFIRM']

		const dataDoc = new GoogleSpreadsheet(
			'1jdeeoxYli6FnDWFxsWNAixwiWI8P1u0euqoNhE__OF4'
		)

		const targetDoc = new GoogleSpreadsheet(
			targetDocId
		)

		/* eslint-disable camelcase */
		await dataDoc.useServiceAccountAuth({
			client_email: sheetData.client_email,
			private_key: sheetData.private_key,
		})
		await targetDoc.useServiceAccountAuth({
			client_email: sheetData.client_email,
			private_key: sheetData.private_key,
		})
		/* eslint-enable camelcase */

		await dataDoc.loadInfo()
		await targetDoc.loadInfo()

		const data = await MySQL.execute(
			`
          SELECT fo.created_at,
                 COALESCE(io.ordered_at, (SELECT fom2.created_at
                                          FROM commerce.fetching_order_memo fom2
                                          WHERE so.shop_order_number = fom2.shop_order_number
                                            AND to_value = 'ORDER_COMPLETE'
                                          ORDER BY fom2.created_at DESC
                                          LIMIT 1))                              orderedAt,
                 CONCAT(si.shop_name, ' ', sp.shop_country)                   AS shopName,
                 CONCAT(bi.brand_name, ' ', COALESCE(ion.name, ii.item_name), ' ', ii.item_code, ' (', ii.idx,
                        ')')                                                  AS itemName,
                 io.quantity                                                  AS quantity,
                 fo.fetching_order_number,
                 io.item_order_number                                         AS itemOrderNumber,
                 io.vendor_order_number                                       AS vendorOrderNumber,
                 ccc.idx                                                      AS cardCompanyName,
                 ccc.name                                                     AS cardCompanyName,
                 io.card_approval_number                                      AS cardApprovalNumber,
                 io.origin_amount                                             AS originAmount,
                 (SELECT SUM(io.origin_amount)
                  FROM commerce.shop_order so2
                           JOIN commerce.item_order io ON so2.shop_order_number = io.shop_order_number
                           LEFT JOIN commerce.order_refund_item ori
                                     ON io.item_order_number = ori.item_order_number AND ori.status = 'ACCEPT'
                  WHERE ori.item_order_number IS NULL
                    AND io.deleted_at IS NULL
                    AND so2.shop_order_number = so.shop_order_number)         AS shopOriginAmount,
                 (SELECT SUM(io.origin_amount)
                  FROM commerce.shop_order so2
                           JOIN commerce.item_order io ON so2.shop_order_number = io.shop_order_number
                  WHERE so2.shop_order_number = so.shop_order_number
                    AND io.deleted_at IS NULL)                                AS fullShopOriginAmount,
                 (SELECT SUM(io.origin_amount)
                  FROM commerce.fetching_order fo2
                           JOIN commerce.shop_order so ON fo2.fetching_order_number = so.fetching_order_number
                           JOIN commerce.item_order io ON so.shop_order_number = io.shop_order_number
                           LEFT JOIN commerce.order_refund_item ori
                                     ON io.item_order_number = ori.item_order_number AND ori.status = 'ACCEPT'
                  WHERE ori.item_order_number IS NULL
                    AND io.deleted_at IS NULL
                    AND fo2.fetching_order_number = fo.fetching_order_number) AS totalOriginAmount,
                 (SELECT SUM(io.origin_amount)
                  FROM commerce.fetching_order fo2
                           JOIN commerce.shop_order so ON fo2.fetching_order_number = so.fetching_order_number
                           JOIN commerce.item_order io ON so.shop_order_number = io.shop_order_number
                  WHERE fo2.fetching_order_number = fo.fetching_order_number
                    AND io.deleted_at IS NULL)                                AS fullTotalOriginAmount,
                 fo.pay_amount                                                AS payAmount,
                 (SELECT JSON_ARRAYAGG(JSON_OBJECT('method', oapi.pay_method, 'amount', oapi.amount))
                  FROM commerce.order_additional_pay oap
                           JOIN commerce.order_additional_pay_item oapi
                                ON oapi.order_additional_number = oap.order_additional_number AND
                                   oapi.status = 'PAID'
                  WHERE oap.fetching_order_number = fo.fetching_order_number) AS additionalPayInfo,
                 fo.status                                                    AS fetchingOrderStatus,
                 io.status                                                    AS status,
                 fo.order_path,
                 fo.pay_method,
                 (SELECT JSON_ARRAYAGG(status)
                  FROM (SELECT case
                                   when oreti2.return_item_number is not null
                                       then '반품'
                                   when oci2.cancel_item_number is not null
                                       then '주문 취소'
                                   when orefi2.refund_item_number is not null
                                       then '주문 취소'
                                   when fo2.status = 'COMPLETE'
                                       then '구매 확정'
                                   when io2.status in ('ORDER_CONFIRM')
                                       then '구매 확정'
                                   when io2.status in ('SHIPPING_COMPLETE')
                                       then '배송 완료'
                                   when io2.status in ('IN_DOMESTIC_SHIPPING')
                                       then '국내 배송 중'
                                   when io2.status in ('CUSTOMS_CLEARANCE_DELAY')
                                       then '통관 지연'
                                   when io2.status in ('DOMESTIC_CUSTOMS_CLEARANCE')
                                       then '국내 통관 중'
                                   when io2.status in ('WAYPOINT_ARRIVAL')
                                       then '경유지 도착'
                                   when io2.status in ('IN_WAYPOINT_SHIPPING')
                                       then '경유지 배송 중'
                                   when io2.status in ('SHIPPING_START')
                                       then '배송 시작'
                                   when io2.status in ('PRODUCT_PREPARE')
                                       then '상품 준비 중'
                                   when io2.status in ('ORDER_DELAY_IN_SHOP')
                                       then '주문 지연'
                                   when io2.status in ('ORDER_COMPLETE')
                                       then '발주 완료'
                                   when io2.status in ('ORDER_DELAY')
                                       then '발주 지연'
                                   when io2.status in ('PRE_ORDER_REQUIRED')
                                       then '선 발주 필요'
                                   when io2.status in ('ORDER_WAITING')
                                       then '발주 대기'
                                   when io2.status in ('ORDER_AVAILABLE')
                                       then '신규 주문'
                                   when io2.status in ('BEFORE_DEPOSIT')
                                       then '입금 전 주문'
                                   else ''
                                   end as status
                        FROM commerce.item_order io2
                                 inner join commerce.shop_order so2
                                            on so2.shop_order_number = io2.shop_order_number
                                 INNER JOIN commerce.fetching_order fo2
                                            ON so2.fetching_order_number = fo2.fetching_order_number
                                 left join commerce.order_cancel_item oci2
                                           on io2.item_order_number = oci2.item_order_number AND
                                              oci2.status = 'ACCEPT' AND oci2.deleted_at IS NULL
                                 left join commerce.order_return_item oreti2
                                           on io2.item_order_number = oreti2.item_order_number AND
                                              oreti2.status = 'ACCEPT' AND oreti2.deleted_at IS NULL
                                 left join commerce.order_refund_item orefi2
                                           on io2.item_order_number = orefi2.item_order_number AND
                                              orefi2.status = 'ACCEPT' AND orefi2.deleted_at IS NULL
                        WHERE so2.fetching_order_number = fo.fetching_order_number
                          AND io2.deleted_at IS NULL
                        GROUP BY io2.item_order_number) t)                    AS itemStatusList,
                 COALESCE(oretec_D.extra_charge, 0)                           AS domesticExtraCharge,
                 COALESCE(oretec_O.extra_charge, 0)                           AS overseasExtraCharge,
                 COALESCE(oretec_R.extra_charge, 0)                           AS repairExtraCharge,
                 (SELECT SUM(point_total)
                  FROM commerce.user_point up
                  WHERE up.user_id = fo.user_id
                    AND up.fetching_order_number = fo.fetching_order_number
                    AND up.save_type IN ('SERVICE_ISSUE', 'DELIVERY_ISSUE'))  AS pointByIssue,
                 COALESCE(
										 IF(oref.created_at > '2022-11-02 14:10:00',
												(SELECT SUM(from_amount) - SUM(to_amount)
												 FROM commerce.order_refund_history orh
												 WHERE orh.order_refund_number = oref.order_refund_number),
												IF(oret.reason_type IS NOT NULL,
												    if(oret.reason_type IN ('DEFECTIVE_PRODUCT', 'WRONG_DELIVERY'),
												        0,
												        ssi.customer_negligence_return_fee
												    ),
												    0
												)
										 ),
										 0
								 )                                                        		AS returnFee,
                 oret.reason_type                                             AS returnReason,
                 case
                     when oreti.return_item_number is not null
                         then '반품'
                     when oci.cancel_item_number is not null
                         then '주문 취소'
                     when orefi.order_refund_number is not null
                         then '주문 취소'
                     when fo.status = 'COMPLETE'
                         then '구매 확정'
                     when io.status in ('ORDER_CONFIRM')
                         then '구매 확정'
                     when io.status in ('SHIPPING_COMPLETE')
                         then '배송 완료'
                     when io.status in ('IN_DOMESTIC_SHIPPING')
                         then '국내 배송 중'
                     when io.status in ('CUSTOMS_CLEARANCE_DELAY')
                         then '통관 지연'
                     when io.status in ('DOMESTIC_CUSTOMS_CLEARANCE')
                         then '국내 통관 중'
                     when io.status in ('WAYPOINT_ARRIVAL')
                         then '경유지 도착'
                     when io.status in ('IN_WAYPOINT_SHIPPING')
                         then '경유지 배송 중'
                     when io.status in ('SHIPPING_START')
                         then '배송 시작'
                     when io.status in ('PRODUCT_PREPARE')
                         then '상품 준비 중'
                     when io.status in ('ORDER_DELAY_IN_SHOP')
                         then '주문 지연'
                     when io.status in ('ORDER_COMPLETE')
                         then '발주 완료'
                     when io.status in ('ORDER_DELAY')
                         then '발주 지연'
                     when io.status in ('PRE_ORDER_REQUIRED')
                         then '선 발주 필요'
                     when io.status in ('ORDER_WAITING')
                         then '발주 대기'
                     when io.status in ('ORDER_AVAILABLE')
                         then '신규 주문'
                     when io.status in ('BEFORE_DEPOSIT')
                         then '입금 전 주문'
                     else ''
                     end                                                      AS itemOrderStatus,
                 scc.name                                                     AS shippingCompany,
                 io.invoice                                                   AS invoice,
                 (SELECT JSON_ARRAYAGG(opcl.data)
                  FROM commerce.order_pay_cancel_log opcl
                  WHERE opcl.fetching_order_number = fo.fetching_order_number
                    AND success)                                              AS refundData,
                 (SELECT JSON_ARRAYAGG(oapcl.data)
                  FROM commerce.order_additional_pay_cancel_log oapcl
                           JOIN commerce.order_additional_pay_log oapl on oapcl.order_additional_pay_log_id = oapl.idx
                           JOIN commerce.order_additional_pay_item oapi
                                ON oapi.additional_item_number = oapl.additional_item_number
                           JOIN commerce.order_additional_pay oap
                                on oap.order_additional_number = oapi.order_additional_number
                  WHERE oap.fetching_order_number = fo.fetching_order_number) AS additionalRefundData,
                 (SELECT JSON_ARRAYAGG(JSON_OBJECT('itemOrderNumber', io2.item_order_number,
                                                   'payAmount', io2.pay_amount_detail,
                                                   'country', sp2.shop_country,
                                                   'isRefunded', EXISTS(SELECT *
                                                                        FROM commerce.order_refund_item ori2
                                                                        WHERE ori2.item_order_number = io2.item_order_number
                                                                          AND ori2.status = 'ACCEPT'
                                                                          AND ori2.deleted_at IS NULL),
                                                   'vatDeductionRate',
                                                   IF(dm2.vat_deduct && sp2.is_vat_deduction_available,
                                                      sp2.vat_deduction_rate, 0),
                                                   'waypointFeeType', dm2.fee_type,
                                                   'waypointFeeWithDeduction', dm2.fee_with_deduction,
                                                   'waypointFeeWithoutDeduction', dm2.fee_without_deduction,
                                                   'minimumWaypointFeeWithDeduction', dm2.minimum_fee_with_deduction,
                                                   'minimumWaypointFeeWithoutDeduction',
                                                   dm2.minimum_fee_without_deduction
                     ))
                  FROM commerce.item_order io2
                           JOIN commerce.shop_order so2 ON io2.shop_order_number = so2.shop_order_number
                           JOIN shop_price sp2 ON so2.shop_id = sp2.idx
                           JOIN delivery_method dm2 on so2.delivery_method = dm2.idx
                  WHERE io2.item_order_number = io.item_order_number
                    AND io2.deleted_at IS NULL)                               AS itemPayAmountDetail,
                 io.inherited_shop_coupon_discount_amount                     AS inheritedShopCouponDiscountAmount,
                 io.inherited_order_coupon_discount_amount                    AS inheritedOrderCouponDiscountAmount,
                 io.inherited_order_use_point                                 AS inheritedOrderUsePoint,
                 io.coupon_discount_amount                                    AS itemCouponDiscountAmount,
                 so.coupon_discount_amount                                    AS shopCouponDiscountAmount,
                 fo.coupon_discount_amount                                    AS orderCouponDiscountAmount,
                 fo.use_point                                                 AS orderPointDiscountAmount,
                 so.is_ddp_service                                            AS isDDP,
                 CONCAT(dm.name, ' ', dm.country)                             AS deliveryMethod,
                 iocm.amount                                                  AS affiliateFee,
                 oci.item_order_number IS NOT NULL OR
                 orefi.item_order_number IS NOT NULL                          AS isCanceled,
                 oreti.item_order_number IS NOT NULL                          AS isReturned
          FROM commerce.fetching_order fo
                   JOIN commerce.shop_order so ON fo.fetching_order_number = so.fetching_order_number
                   JOIN commerce.item_order io on so.shop_order_number = io.shop_order_number
                   LEFT JOIN commerce.order_cancel_item oci
                             on io.item_order_number = oci.item_order_number AND oci.status = 'ACCEPT' AND oci.deleted_at IS NULL
                   LEFT JOIN commerce.order_cancel oc on oci.order_cancel_number = oc.order_cancel_number
                   LEFT JOIN commerce.order_return_item oreti
                             on oreti.item_order_number = io.item_order_number AND oreti.status = 'ACCEPT' AND oreti.deleted_at IS NULL
                   LEFT JOIN commerce.order_return oret on oreti.order_return_number = oret.order_return_number
                   LEFT JOIN commerce.order_return_extra_charge oretec_D
                             ON oretec_D.order_return_number = oret.order_return_number AND
                                oretec_D.reason_type = 'DOMESTIC_RETURN'
                   LEFT JOIN commerce.order_return_extra_charge oretec_O
                             ON oretec_O.order_return_number = oret.order_return_number AND
                                oretec_O.reason_type = 'OVERSEAS_RETURN'
                   LEFT JOIN commerce.order_return_extra_charge oretec_R
                             ON oretec_R.order_return_number = oret.order_return_number AND
                                oretec_R.reason_type = 'REPAIR'
                   LEFT JOIN commerce.order_refund_item orefi
                             on orefi.item_order_number = io.item_order_number AND orefi.status = 'ACCEPT' AND orefi.deleted_at IS NULL
                   LEFT JOIN commerce.order_refund oref on orefi.order_refund_number = oref.order_refund_number
                   LEFT JOIN commerce.shipping_company_code scc ON scc.code = io.shipping_code
                   LEFT JOIN commerce.credit_card_company ccc ON ccc.idx = io.card_company_id
                   JOIN fetching_dev.delivery_method dm ON so.delivery_method = dm.idx
                   LEFT JOIN commerce.item_order_commission_map iocm ON io.item_order_number = iocm.item_order_number
                   JOIN shop_price sp on so.shop_id = sp.idx
                   JOIN fetching_dev.item_info ii ON ii.idx = io.item_id
                   JOIN fetching_dev.shop_info si ON sp.shop_id = si.shop_id
                   LEFT JOIN brand_info bi on ii.brand_id = bi.brand_id
                   LEFT JOIN item_original_name ion on ii.idx = ion.item_id
                   LEFT JOIN shop_support_info ssi ON si.shop_id = ssi.shop_id
          WHERE fo.paid_at IS NOT NULL
            AND fo.deleted_at IS NULL
            AND io.deleted_at IS NULL
            AND (
                      (fo.created_at + INTERVAL 9 HOUR) >= ?
                  AND
                      (fo.created_at + INTERVAL 9 HOUR) < ?
              )
          GROUP BY io.item_order_number
          ORDER BY fo.created_at ASC
			`,
			[start, end]
		)

		const feed = data.map((row) => {
			//row.itemOrderNumber = row.itemOrderNumber.map(itemOrder => [itemOrder.itemOrderNumber, itemOrder.orderedAt ? `(${DateTime.fromISO(row.created_at.toISOString()).setZone('Asia/Seoul').toFormat('yyyy-MM-dd HH:mm:ss')})` : ''].join(' ').trim()).join(', ')
			// row.itemOrderNumber = row.itemOrderNumber.join(', ')
			row.additionalPayAmount = 0

			const refundData = [...row.refundData ?? [], ...row.additionalRefundData ?? []]
				.map(data => JSON.parse(data)).filter(data => {
					return data?.ResultCode === '2001'
				})

			let refundAmount = refundData.reduce(((a: number, b: any) => a + parseInt(b.CancelAmt)), 0)
			if (!(row.isCanceled || row.isReturned)) refundAmount = 0
			// const salesAmount = row.payAmount - refundAmount
			// const totalPrice = priceData.SHOP_PRICE_KOR + priceData.DUTY_AND_TAX + priceData.DELIVERY_FEE
			// const totalTotalPrice = !row.refundAmount ? (totalPrice + pgFee - refundAmount) : 0

			// const profit = salesAmount - totalTotalPrice

			const statusCount = {}
			// '반품', '주문 취소', '구매 확정', '주문 완료'
			for (const status of row.itemStatusList) {
				if (!statusCount[status]) statusCount[status] = 0
				statusCount[status]++
			}

			let cancelCount = statusCount['주문 취소'] ?? 0
			let returnCount = statusCount['반품'] ?? 0


			let pgFee = 0

			if (row.pay_method === 'CARD') {
				pgFee = Math.round(row.payAmount * 0.014)
			} else if (row.pay_method === 'KAKAO') {
				pgFee = Math.round(row.payAmount * 0.015)
			} else if (row.pay_method === 'NAVER') {
				pgFee = Math.round(row.payAmount * 0.015)
			} else if (row.pay_method === 'ESCROW') {
				pgFee = Math.round(row.payAmount * 0.017)
			} else if (row.pay_method === 'ESCROW_CARD') {
				pgFee = Math.round(row.payAmount * 0.016)
			}

			if (row.additionalPayInfo) {
				for (const {amount, method} of row.additionalPayInfo) {
					row.additionalPayAmount += amount
					if (method === 'CARD') pgFee += Math.round(amount * 0.014)
					else if (method === 'KAKAO') pgFee += Math.round(amount * 0.015)
					else if (method === 'NAVER') pgFee += Math.round(amount * 0.015)
					else if (method === 'ESCROW') pgFee += Math.round(amount * 0.017)
					else if (method === 'ESCROW_CARD')
						pgFee += Math.round(amount * 0.016)
				}
			}

			const refundPgFee = refundData.reduce((a, b) => {
				const amount = parseInt(b.CancelAmt)
				let pgFee = 0
				if (b.PayMethod === 'CARD') pgFee = Math.round(amount * 0.014)
				else if (b.PayMethod === 'KAKAO') pgFee = Math.round(amount * 0.015)
				else if (b.PayMethod === 'NAVER') pgFee = Math.round(amount * 0.015)
				else if (b.PayMethod === 'ESCROW') pgFee = Math.round(amount * 0.017)
				else if (b.PayMethod === 'ESCROW_CARD')
					pgFee = Math.round(amount * 0.016)
				return a + pgFee
			}, 0)

			const itemPriceData: any = {}
			const itemPriceDataCanceled: any = {}
			row.itemPayAmountDetail.map((detail) => {
				const currentItemPriceData: any = {}
				let count = 0
				const payAmountDetailData = Object.entries<any>(JSON.parse(detail.payAmount))
				payAmountDetailData.forEach(([key, data]) => {
					if (key !== detail.country && (detail.country === 'KR' ? count !== 0 : true) && (key === 'KR' && payAmountDetailData.length === 1 ? count !== 0 : true)) return
					count++
					data.forEach((_row) => {
						if (!localStatus.includes(row.status) && _row.type === 'DUTY_AND_TAX') {
							itemPriceData[_row.type] = 0
							itemPriceDataCanceled[_row.type] = 0
						}
						if (itemPriceData[_row.type]) {
							itemPriceData[_row.type] += _row.rawValue * row.quantity
						} else {
							itemPriceData[_row.type] = _row.rawValue * row.quantity
							itemPriceDataCanceled[_row.type] = 0
						}
						if (currentItemPriceData[_row.type]) {
							currentItemPriceData[_row.type] += _row.rawValue * row.quantity
						} else {
							currentItemPriceData[_row.type] = _row.rawValue * row.quantity
						}
						if (detail.isRefunded) {
							if (itemPriceDataCanceled[_row.type])
								itemPriceDataCanceled[_row.type] += _row.rawValue * row.quantity
							else itemPriceDataCanceled[_row.type] = _row.rawValue * row.quantity
						}
					})
				})

				let deductedVat
				if (isNil(currentItemPriceData['DEDUCTED_VAT'])) {
					const fasstoPurchaseAmount = currentItemPriceData['SHOP_PRICE_KOR'] + currentItemPriceData['DELIVERY_FEE']
					deductedVat = Math.round(fasstoPurchaseAmount - fasstoPurchaseAmount / (1 + detail.vatDeductionRate))
					currentItemPriceData['DEDUCTED_VAT'] = deductedVat
					if (itemPriceData['DEDUCTED_VAT'])
						itemPriceData['DEDUCTED_VAT'] += deductedVat
					else itemPriceData['DEDUCTED_VAT'] = deductedVat
				}

				if (isNil(currentItemPriceData['WAYPOINT_FEE'])) {
					let waypointFee = 0
					const waypointCurrencyRate = currentItemPriceData['SHOP_PRICE_KOR'] / currentItemPriceData['SHOP_PRICE']

					switch (detail.waypointFeeType) {
					case 'PERCENT':
						waypointFee = Math.round((currentItemPriceData['SHOP_PRICE_KOR'] - deductedVat) * (deductedVat ? detail.waypointFeeWithDeduction : detail.waypointFeeWithoutDeduction))
						break
					case 'FIXED':
						waypointFee = Math.round((deductedVat ? detail.waypointFeeWithDeduction : detail.waypointFeeWithoutDeduction) * waypointCurrencyRate)
						break
					}

					if (deductedVat && detail.waypointMinimumFeeWithDeduction * waypointCurrencyRate > waypointFee) {
						waypointFee = detail.waypointMinimumFeeWithDeduction * waypointCurrencyRate
					} else if (!deductedVat && detail.waypointMinimumFeeWithoutDeduction * waypointCurrencyRate > waypointFee) {
						waypointFee = detail.waypointMinimumFeeWithDeduction * waypointCurrencyRate
					}

					currentItemPriceData['WAYPOINT_FEE'] = waypointFee
					if (itemPriceData['WAYPOINT_FEE'])
						itemPriceData['WAYPOINT_FEE'] = waypointFee
					else itemPriceData['WAYPOINT_FEE'] = waypointFee
				}
			})

			let canceledDeductedVat = 0, canceledWaypointFee = 0

			if (row.isCanceled && !localStatus.includes(row.status)) {
				canceledDeductedVat = itemPriceData['DEDUCTED_VAT']
				canceledWaypointFee = itemPriceData['WAYPOINT_FEE']
			}

			let canceledAdditionalFee = 0

			if ((row.isCanceled && beforeShippingStatus.includes(row.status)) || row.isReturned) {
				canceledAdditionalFee = itemPriceData['ADDITIONAL_FEE']
			}

			const purchaseValue = itemPriceData['SHOP_PRICE_KOR'] + itemPriceData['DELIVERY_FEE'] + (row.isDDP ? itemPriceData['DUTY_AND_TAX'] : 0)
			const lCardRefundValue = itemPriceDataCanceled['SHOP_PRICE_KOR'] + itemPriceDataCanceled['DELIVERY_FEE'] - (itemPriceData['DEDUCTED_VAT'] ?? 0) + (itemPriceDataCanceled['DUTY_AND_TAX']) + (itemPriceDataCanceled['WAYPOINT_FEE'] ?? 0) + (itemPriceDataCanceled['ADDITIONAL_FEE'] || 0) + itemPriceDataCanceled['FETCHING_FEE']

			let coupon = 0, point = 0

			if (!isNil(row.itemCouponDiscountAmount)) {
				coupon += row.itemCouponDiscountAmount
			}

			if (isNil(row.inheritedShopCouponDiscountAmount)) {
				const shopCouponDiscountAmount = Math.round(row.shopCouponDiscountAmount * (row.originAmount / (row.shopOriginAmount ?? row.fullShopOriginAmount)))
				if (shopCouponDiscountAmount) coupon += shopCouponDiscountAmount
			} else {
				coupon += row.inheritedShopCouponDiscountAmount
			}

			if (isNil(row.inheritedOrderCouponDiscountAmount)) {
				const orderCouponDiscountAmount = Math.round(row.orderCouponDiscountAmount * (row.originAmount / (row.totalOriginAmount ?? row.fullTotalOriginAmount)))
				if (orderCouponDiscountAmount) coupon += orderCouponDiscountAmount
			} else {
				coupon += row.inheritedOrderCouponDiscountAmount
			}

			if (isNil(row.inheritedOrderUsePoint)) {
				point += Math.round(row.orderPointDiscountAmount * (row.originAmount / (row.totalOriginAmount ?? row.fullTotalOriginAmount)))
			} else {
				point += row.inheritedOrderUsePoint
			}

			let canceledCoupon = 0, canceledPoint = 0, canceledFetchingFee = 0

			if ((row.isCanceled || row.isReturned)) {
				canceledCoupon = coupon
				canceledPoint = point
				canceledFetchingFee = itemPriceData['FETCHING_FEE']
			}

			const data = {
				주문일: DateTime.fromISO(row.created_at.toISOString()).setZone('Asia/Seoul').toFormat('yyyy-MM-dd HH:mm:ss'),
				'주문 상태': row.itemOrderStatus,
				'상품별 주문번호': row.itemOrderNumber,
				'카드 승인번호': row.cardApprovalNumber?.replace('매입', ''),
				'전체 주문번호': row.fetching_order_number,
				'전체 주문 상태': Object.entries(statusCount).map(([key, value]) => `${key} ${value}`).join(', '),
				'편집샵 매입 주문번호': row.vendorOrderNumber,
				'카드사': row.cardCompanyName,
				'운송 업체': row.shippingCompany,
				'운송장 번호': row.invoice,
				'배송 유형': row.deliveryMethod,
				'세금 부과 방식': row.isDDP ? 'DDP' : 'DDU',
				'편집샵명': row.shopName,
				'결제 방식': row.pay_method,
				'상품명': row.itemName,
				'수량': row.quantity,
				'상품 원가': purchaseValue,
				'부가세 환급': itemPriceData['DEDUCTED_VAT'],
				'관부가세': (row.isDDP ? 0 : itemPriceData['DUTY_AND_TAX']),
				'운송료': itemPriceData['ADDITIONAL_FEE'] || 0,
				'페칭 수수료': itemPriceData['FETCHING_FEE'],
				'부가세 환급 수수료': itemPriceData['WAYPOINT_FEE'],
				'쿠폰': coupon,
				'적립금': point,
				'PG수수료': pgFee - refundPgFee,
				'실 결제 금액': row.payAmount,
				'차액 결제 금액': parseInt(row.additionalPayAmount) || 0,
				'결제 환불 금액': refundAmount,
				'관부가세 환급': ((row.isCanceled || row.isReturned) && row.invoice && row.isDDP) ? itemPriceData['DUTY_AND_TAX'] : 0,
				'페칭 수수료 취소': canceledFetchingFee,
				'부가세 환급 취소': canceledDeductedVat,
				'부가세 환급 수수료 취소': canceledWaypointFee,
				'쿠폰 반환': canceledCoupon,
				'적립금 반환': canceledPoint,
				'운송료 취소': canceledAdditionalFee,
				'국내 반품 비용': row.domesticExtraCharge,
				'해외 반품 비용': row.overseasExtraCharge,
				'보상 적립금': row.pointByIssue,
				'수선 비용': row.repairExtraCharge,
				'매입 환출 금액': (row.isCanceled || row.isReturned) ? Math.abs(purchaseValue) : 0, // -cardRefundValue,
				'반품 수수료': row.returnFee,
				'제휴 수수료': ''
			}

			return data
		})

		let targetSheet = targetDoc.sheetsById[targetSheetId]
		const rows = await targetSheet.getRows()
		// @ts-ignore
		await targetSheet.loadCells()
		let hasModified = false
		for (const i in feed) {
			if (rows[i]) {
				for (const key of Object.keys(feed[i])) {
					if (['수동 확인 필요'].includes(feed[i][key])) continue
					if (isEmpty(rows[i][key]) && isEmpty(feed[i][key]) && rows[i][key] === '' && (isNil(feed[i][key]) || feed[i][key] === '')) continue
					if (rows[i][key] === (isString(feed[i][key]) ? feed[i][key] : feed[i][key]?.toString())) continue
					if ((rows[i][key] === (isDate(feed[i][key]) ? feed[i][key]?.toISOString() : feed[i][key]))) continue
					if (!['주문일', '구매확정일'].includes(key) && (parseFloat(rows[i][key]?.replace(/,/g, '')) === (isNaN(parseFloat(feed[i][key])) ? feed[i][key] : parseFloat(feed[i][key])))) continue

					const cell = targetSheet.getCell(rows[i].rowIndex - 1, targetSheet.headerValues.indexOf(key))

					if (cell?.effectiveFormat?.backgroundColor) {
						const {red, green, blue} = cell.effectiveFormat.backgroundColor
						if (!(red === 1 && green === 1 && blue === 1)) continue
					}
					if (cell.effectiveFormat?.numberFormat?.type?.includes('DATE')) cell.effectiveFormat.numberFormat.type = 'TEXT'

					try {
						cell.value = feed[i][key]
					} catch (e) {
						console.log(key, feed[i])
						throw e
					}
					hasModified = true
				}
			} else {
				await retry(3, 3000)(async () => {
					await targetSheet.addRow(feed[i], {raw: true, insert: true})
				})
				await sleep(500)
			}
		}
		if (hasModified) {
			await retry(3, 3000)(async () => {
				await targetSheet.saveUpdatedCells()
			})
		}

		return parse(feed, {
			fields: Object.keys(feed[0]),
			delimiter: ',',
			quote: '"',
		})
	}

	async upload() {
		console.log(
			await S3Client.upload({
				folderName: 'feeds',
				fileName: '2023년_7월_추정.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2023-07-01T00:00:00.000Z'),
					new Date('2023-08-01T00:00:00.000Z'),
					'1jdeeoxYli6FnDWFxsWNAixwiWI8P1u0euqoNhE__OF4',
					'1530152555'
				),
				contentType: 'text/csv',
			})
		)
	}
}
