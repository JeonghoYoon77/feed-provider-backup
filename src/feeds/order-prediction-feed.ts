import {Calculate, retry, sleep} from '@fetching-korea/common-utils'
import {GoogleSpreadsheet} from 'google-spreadsheet'
import {parse} from 'json2csv'
import {isDate, isEmpty, isNil, isString, parseInt} from 'lodash'
import {DateTime} from 'luxon'

import sheetData from '../../fetching-sheet.json'

import {MySQL, S3Client} from '../utils'

import {iFeed} from './feed'

export class OrderPredictionFeed implements iFeed {
	async getTsvBufferWithRange(start: Date, end: Date, targetSheetId = null): Promise<Buffer> {
		return Buffer.from(await this.getTsv({start, end, targetSheetId}), 'utf-8')
	}

	async getTsvBuffer(): Promise<Buffer> {
		return Buffer.from(await this.getTsv({start: null, end: null}), 'utf-8')
	}

	async getTsv({start, end, targetSheetId}: { start: Date, end: Date, targetSheetId?: string }): Promise<string> {
		const targetDoc = new GoogleSpreadsheet(
			'1hmp69Ej9Gr4JU1KJ6iHO-iv1Tga5lMyp8NO5-yRDMlU'
		)

		/* eslint-disable camelcase */
		await targetDoc.useServiceAccountAuth({
			client_email: sheetData.client_email,
			private_key: sheetData.private_key,
		})
		/* eslint-enable camelcase */

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
                 io.card_approval_number                                      AS cardApprovalNumber,
                 io.origin_amount                                             AS originAmount,
                 (SELECT SUM(io.origin_amount)
                  FROM commerce.shop_order so2
                           JOIN commerce.item_order io ON so2.shop_order_number = io.shop_order_number
                           LEFT JOIN commerce.order_refund_item ori
                                     ON io.item_order_number = ori.item_order_number AND ori.status = 'ACCEPT'
                  WHERE ori.item_order_number IS NULL
                    AND so2.shop_order_number = so.shop_order_number)         AS shopOriginAmount,
                 (SELECT SUM(io.origin_amount)
                  FROM commerce.shop_order so2
                           JOIN commerce.item_order io ON so2.shop_order_number = io.shop_order_number
                  WHERE so2.shop_order_number = so.shop_order_number)         AS fullShopOriginAmount,
                 (SELECT SUM(io.origin_amount)
                  FROM commerce.fetching_order fo2
                           JOIN commerce.shop_order so ON fo2.fetching_order_number = so.fetching_order_number
                           JOIN commerce.item_order io ON so.shop_order_number = io.shop_order_number
                           LEFT JOIN commerce.order_refund_item ori
                                     ON io.item_order_number = ori.item_order_number AND ori.status = 'ACCEPT'
                  WHERE ori.item_order_number IS NULL
                    AND fo2.fetching_order_number = fo.fetching_order_number) AS totalOriginAmount,
                 (SELECT SUM(io.origin_amount)
                  FROM commerce.fetching_order fo2
                           JOIN commerce.shop_order so ON fo2.fetching_order_number = so.fetching_order_number
                           JOIN commerce.item_order io ON so.shop_order_number = io.shop_order_number
                  WHERE fo2.fetching_order_number = fo.fetching_order_number) AS fullTotalOriginAmount,
                 fo.pay_amount                                                AS payAmount,
                 (
                     SELECT JSON_ARRAYAGG(JSON_OBJECT('method', oapi.pay_method, 'amount', oapi.amount))
                     FROM commerce.order_additional_pay oap
                              JOIN commerce.order_additional_pay_item oapi
                                   ON oapi.order_additional_number = oap.order_additional_number AND
                                      oapi.status = 'PAID'
                     WHERE oap.fetching_order_number = fo.fetching_order_number
                 )                                                            AS additionalPayInfo,
                 fo.status,
                 fo.order_path,
                 fo.pay_method,
                 (
                     SELECT JSON_ARRAYAGG(
                                    case
                                        when ori2.status = 'ACCEPT'
                                            then '반품'
                                        when ori2.status = 'IN_PROGRESS'
                                            then '반품 진행 중'
                                        when ori2.status = 'HOLD'
                                            then '반품 보류'
                                        when oci2.cancel_item_number is not null
                                            then '주문 취소'
                                        when fo.status = 'COMPLETE'
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
                                        end
                                )
                     FROM commerce.item_order io2
                              inner join commerce.shop_order so2
                                         on so2.shop_order_number = io2.shop_order_number
                              left join commerce.order_cancel_item oci2
                                        on io2.item_order_number = oci2.item_order_number AND
                                           oci2.status = 'ACCEPT'
                              left join commerce.order_return_item ori2
                                        on io2.item_order_number = ori2.item_order_number AND
                                           ori2.status IN ('IN_PROGRESS', 'HOLD', 'ACCEPT')
                     WHERE so2.fetching_order_number = fo.fetching_order_number
                 )                                                            AS itemStatusList,
                 ssi.customer_negligence_return_fee                           AS returnFee,
                 oret.reason_type                                             AS returnReason,
                 case
                     when oreti.return_item_number is not null
                         then '반품'
                     when oci.cancel_item_number is not null
                         then '주문 취소'
                     when fo.status = 'COMPLETE'
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
                           JOIN commerce.order_additional_pay_cancel_log_item_map oapclim
                                ON oapcl.idx = oapclim.cancel_log_id
                           JOIN commerce.order_additional_pay_log oapl on oapcl.order_additional_pay_log_id = oapl.idx
                           JOIN commerce.order_additional_pay_item oapi
                                ON oapi.additional_item_number = oapl.additional_item_number
                           JOIN commerce.order_additional_pay oap
                                on oap.order_additional_number = oapi.order_additional_number
                  WHERE oap.fetching_order_number = fo.fetching_order_number) AS additionalRefundData,
                 (SELECT JSON_ARRAYAGG(JSON_OBJECT('itemOrderNumber', io2.item_order_number,
                                                   'payAmount', io2.pay_amount_detail,
                                                   'country', sp2.shop_country,
                                                   'isRefunded', ori2.item_order_number IS NOT NULL,
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
                           LEFT JOIN commerce.order_refund_item ori2
                                     ON ori2.item_order_number = io2.item_order_number AND ori2.status = 'ACCEPT' AND
                                        ori2.deleted_at IS NULL
                           JOIN commerce.shop_order so2 ON io2.shop_order_number = so2.shop_order_number
                           JOIN shop_price sp2 ON so2.shop_id = sp2.idx
                           JOIN delivery_method dm2 on so2.delivery_method = dm2.idx
                  WHERE io2.item_order_number = io.item_order_number)         AS itemPayAmountDetail,
                 (SELECT SUM(amount)
                  FROM commerce.order_additional_pay_item oapi
                  WHERE oapi.item_order_number = io.item_order_number
                    AND oapi.status = 'PAID')                                 AS additionalPayAmount,
                 io.inherited_shop_coupon_discount_amount                     AS inheritedShopCouponDiscountAmount,
                 io.inherited_order_coupon_discount_amount                    AS inheritedOrderCouponDiscountAmount,
                 io.inherited_order_use_point                                 AS inheritedOrderUsePoint,
                 io.coupon_discount_amount                                    AS itemCouponDiscountAmount,
                 so.coupon_discount_amount                                    AS shopCouponDiscountAmount,
                 fo.coupon_discount_amount                                    AS orderCouponDiscountAmount,
                 fo.use_point                                                 AS orderPointDiscountAmount,
                 so.is_ddp_service                                            AS isDDP,
                 CONCAT(dm.name, ' ', dm.country)                             AS deliveryMethod,
                 iocm.amount                                                  AS affiliateFee
          FROM commerce.fetching_order fo
                   JOIN commerce.shop_order so ON fo.fetching_order_number = so.fetching_order_number
                   JOIN commerce.item_order io on so.shop_order_number = io.shop_order_number
                   LEFT JOIN commerce.order_cancel_item oci
                             on io.item_order_number = oci.item_order_number AND oci.status = 'ACCEPT'
                   LEFT JOIN commerce.order_cancel oc on oci.order_cancel_number = oc.order_cancel_number
                   LEFT JOIN commerce.order_return_item oreti
                             on oreti.item_order_number = io.item_order_number AND oreti.status = 'ACCEPT'
                   LEFT JOIN commerce.order_return oret on oreti.order_return_number = oret.order_return_number
                   LEFT JOIN commerce.order_refund_item orefi
                             on orefi.item_order_number = io.item_order_number AND orefi.status = 'ACCEPT'
                   LEFT JOIN commerce.order_refund oref on orefi.order_refund_number = oref.order_refund_number
                   LEFT JOIN commerce.shipping_company_code scc ON scc.code = io.shipping_code
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
			const refundData = [...row.refundData ?? [], ...row.additionalRefundData ?? []]
				.map(data => JSON.parse(data)).filter(data => {
					return data?.ResultCode === '2001'
				})

			const refundAmount = refundData.reduce(((a: number, b: any) => a + parseInt(b.CancelAmt)), 0)
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
					row.payAmount += amount
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
				Object.entries<any>(JSON.parse(detail.payAmount)).forEach(([key, data]) => {
					if (key !== detail.country && (detail.country === 'KR' ? count !== 0 : true)) return
					count++
					data.forEach((row) => {
						if (itemPriceData[row.type]) {
							itemPriceData[row.type] += row.rawValue
						} else {
							itemPriceData[row.type] = row.rawValue
							itemPriceDataCanceled[row.type] = 0
						}
						if (currentItemPriceData[row.type]) {
							currentItemPriceData[row.type] += row.rawValue
						} else {
							currentItemPriceData[row.type] = row.rawValue
						}
						if (detail.isRefunded) {
							if (itemPriceDataCanceled[row.type])
								itemPriceDataCanceled[row.type] += row.rawValue
							else itemPriceDataCanceled[row.type] = row.rawValue
						}
					})
				})

				let deductedVat
				if (isNil(currentItemPriceData['DEDUCTED_VAT'])) {
					const fasstoPurchaseAmount = currentItemPriceData['SHOP_PRICE_KOR'] + currentItemPriceData['DELIVERY_FEE']
					deductedVat = Calculate.cut(fasstoPurchaseAmount - fasstoPurchaseAmount / (1 + detail.vatDeductionRate))
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
						waypointFee = Calculate.cut((currentItemPriceData['SHOP_PRICE_KOR'] - deductedVat) * (deductedVat ? detail.waypointFeeWithDeduction : detail.waypointFeeWithoutDeduction))
						break
					case 'FIXED':
						waypointFee = Calculate.cut((deductedVat ? detail.waypointFeeWithDeduction : detail.waypointFeeWithoutDeduction) * waypointCurrencyRate)
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

			if (
				['DEFECTIVE_PRODUCT', 'WRONG_DELIVERY'].includes(row.returnReason) ||
				!returnCount
			) {
				row.returnFee = 0
			}

			const purchaseValue = itemPriceData['SHOP_PRICE_KOR'] + itemPriceData['DELIVERY_FEE'] - (itemPriceData['DEDUCTED_VAT'] ?? 0) + (row.isDDP ? itemPriceData['DUTY_AND_TAX'] : 0) + (itemPriceData['WAYPOINT_FEE'] ?? 0)
			const lCardRefundValue = itemPriceDataCanceled['SHOP_PRICE_KOR'] + itemPriceDataCanceled['DELIVERY_FEE'] - (itemPriceData['DEDUCTED_VAT'] ?? 0) + (itemPriceDataCanceled['DUTY_AND_TAX']) + (itemPriceDataCanceled['WAYPOINT_FEE'] ?? 0) + (itemPriceDataCanceled['ADDITIONAL_FEE'] || 0) + itemPriceDataCanceled['FETCHING_FEE']

			let coupon = 0, point = 0

			if (row.itemOrderNumber === 'P-20220125-0000014') {
				console.log(row.itemCouponDiscountAmount, row.shopCouponDiscountAmount, row.inheritedShopCouponDiscountAmount, row.orderCouponDiscountAmount, row.inheritedOrderCouponDiscountAmount)
			}

			if (!isNil(row.itemCouponDiscountAmount)) {
				coupon += row.itemCouponDiscountAmount
			}

			if (isNil(row.inheritedShopCouponDiscountAmount)) {
				const shopCouponDiscountAmount = Calculate.cut(row.shopCouponDiscountAmount * (row.originAmount / (row.shopOriginAmount ?? row.fullShopOriginAmount)), 1)
				if (shopCouponDiscountAmount) coupon += shopCouponDiscountAmount
			} else {
				coupon += row.inheritedShopCouponDiscountAmount
			}

			if (isNil(row.inheritedOrderCouponDiscountAmount)) {
				const orderCouponDiscountAmount = Calculate.cut(row.orderCouponDiscountAmount * (row.originAmount / (row.totalOriginAmount ?? row.fullTotalOriginAmount)), 1)
				if (orderCouponDiscountAmount) coupon += orderCouponDiscountAmount
			} else {
				coupon += row.inheritedOrderCouponDiscountAmount
			}

			if (isNil(row.inheritedOrderUsePoint)) {
				point += Calculate.cut(row.orderPointDiscountAmount * (row.originAmount / (row.totalOriginAmount ?? row.fullTotalOriginAmount)), 1)
			} else {
				point += row.inheritedOrderUsePoint
			}

			const data = {
				주문일: DateTime.fromISO(row.created_at.toISOString()).setZone('Asia/Seoul').toFormat('yyyy-MM-dd HH:mm:ss'),
				'주문 상태': row.itemOrderStatus,
				'상품별 주문번호': row.itemOrderNumber,
				'카드 승인번호': row.cardApprovalNumber?.replace('매입', ''),
				'전체 주문번호': row.fetching_order_number,
				'전체 주문 상태': Object.entries(statusCount).map(([key, value]) => `${key} ${value}`).join(', '),
				'편집샵 매입 주문번호': row.vendorOrderNumber,
				'카드사': '롯데카드',
				'운송 업체': row.shippingCompany,
				'운송장 번호': row.invoice,
				'배송 유형': row.deliveryMethod,
				'세금 부과 방식': row.isDDP ? 'DDP' : 'DDU',
				'편집샵명': row.shopName,
				'결제 방식': row.pay_method,
				'상품명': row.itemName,
				'수량': row.quantity,
				'상품 원가': purchaseValue,
				'차액 결제 금액': parseInt(row.additionalPayAmount) || 0,
				'관부가세': (row.isDDP ? 0 : itemPriceData['DUTY_AND_TAX']),
				'운송료': itemPriceData['ADDITIONAL_FEE'] || 0,
				'페칭 수수료': itemPriceData['FETCHING_FEE'],
				'쿠폰': coupon,
				'적립금': point,
				'PG수수료': pgFee,
				'실 결제 금액': row.payAmount,
				'결제 환불 금액': refundAmount,
				'관부가세 환급': (cancelCount || returnCount) ? (row.isDDP ? 0 : itemPriceData['DUTY_AND_TAX']) : 0,
				'PG수수료 환불': refundPgFee,
				// '국내 반품 비용': '',
				// 'IBP 반품 비용': '',
				// '보상 적립금': '',
				// '수선 비용': '',
				'매입 환출 금액': (cancelCount || returnCount) ? Math.abs(lCardRefundValue) : 0, // -cardRefundValue,
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
					if (['카드사'].includes(key)) continue
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

					//console.log(key, feed[i]['상품별 주문번호'], feed[i][key])

					cell.value = feed[i][key]
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
				fileName: '1월_추정.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2022-01-01T00:00:00.000Z'),
					new Date('2022-02-01T00:00:00.000Z'),
					'1578509603'
				),
				contentType: 'text/csv',
			})
		)

		console.log(
			await S3Client.upload({
				folderName: 'feeds',
				fileName: '2월_추정.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2022-02-01T00:00:00.000Z'),
					new Date('2022-03-01T00:00:00.000Z'),
					'874794911'
				),
				contentType: 'text/csv',
			})
		)

		console.log(
			await S3Client.upload({
				folderName: 'feeds',
				fileName: '3월_추정.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2022-03-01T00:00:00.000Z'),
					new Date('2022-04-01T00:00:00.000Z'),
					'1181432129'
				),
				contentType: 'text/csv',
			})
		)

		console.log(
			await S3Client.upload({
				folderName: 'feeds',
				fileName: '4월_추정.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2022-04-01T00:00:00.000Z'),
					new Date('2022-05-01T00:00:00.000Z'),
					'1469564260'
				),
				contentType: 'text/csv',
			})
		)

		console.log(
			await S3Client.upload({
				folderName: 'feeds',
				fileName: '5월_추정.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2022-05-01T00:00:00.000Z'),
					new Date('2022-06-01T00:00:00.000Z'),
					'138430318'
				),
				contentType: 'text/csv',
			})
		)

		console.log(
			await S3Client.upload({
				folderName: 'feeds',
				fileName: '6월_추정.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2022-06-01T00:00:00.000Z'),
					new Date('2022-07-01T00:00:00.000Z'),
					'1707621899'
				),
				contentType: 'text/csv',
			})
		)

		console.log(
			await S3Client.upload({
				folderName: 'feeds',
				fileName: '7월_추정.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2022-07-01T00:00:00.000Z'),
					new Date('2022-08-01T00:00:00.000Z'),
					'1307994268'
				),
				contentType: 'text/csv',
			})
		)

		console.log(
			await S3Client.upload({
				folderName: 'feeds',
				fileName: '8월_추정.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2022-08-01T00:00:00.000Z'),
					new Date('2022-09-01T00:00:00.000Z'),
					'734978288'
				),
				contentType: 'text/csv',
			})
		)

		console.log(
			await S3Client.upload({
				folderName: 'feeds',
				fileName: '9월_추정.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2022-09-01T00:00:00.000Z'),
					new Date('2022-10-01T00:00:00.000Z'),
					'479850677'
				),
				contentType: 'text/csv',
			})
		)

		console.log(
			await S3Client.upload({
				folderName: 'feeds',
				fileName: '10월_추정.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2022-10-01T00:00:00.000Z'),
					new Date('2022-11-01T00:00:00.000Z'),
					'1887072688'
				),
				contentType: 'text/csv',
			})
		)
	}
}
