import {Calculate, retry, sleep} from '@fetching-korea/common-utils'
import {GoogleSpreadsheet} from 'google-spreadsheet'
import {parse} from 'json2csv'
import {isDate, isEmpty, isNil, isString, parseInt} from 'lodash'
import {DateTime} from 'luxon'

import sheetData from '../../fetching-sheet.json'

import {MySQL, S3Client} from '../utils'

import {iFeed} from './feed'

export class OrderActualFeed implements iFeed {
	async getTsvBufferWithRange(start: Date, end: Date, targetSheetId = null): Promise<Buffer> {
		return Buffer.from(await this.getTsv({start, end, targetSheetId}), 'utf-8')
	}

	async getTsvBuffer(): Promise<Buffer> {
		return Buffer.from(await this.getTsv({start: null, end: null}), 'utf-8')
	}

	async getTsv({start, end, targetSheetId}: { start: Date, end: Date, targetSheetId?: string }): Promise<string> {
		const beforeShippingStatus = ['BEFORE_DEPOSIT', 'ORDER_AVAILABLE', 'ORDER_WAITING', 'PRE_ORDER_REQUIRED', 'ORDER_COMPLETE', 'ORDER_DELAY', 'ORDER_DELAY_IN_SHOP', 'PRODUCT_PREPARE']
		const overseasStatus = ['SHIPPING_START', 'IN_WAYPOINT_SHIPPING', 'WAYPOINT_ARRIVAL']
		const localStatus = ['DOMESTIC_CUSTOMS_CLEARANCE', 'CUSTOMS_CLEARANCE_DELAY', 'IN_DOMESTIC_SHIPPING', 'SHIPPING_COMPLETE', 'ORDER_CONFIRM']

		// 재무 시트
		const taxDoc = new GoogleSpreadsheet(
			'1SoZM_RUVsuIMyuJdOzWYmwirSb-2c0-5peEPm9K0ATU'
		)
		const vatRefundDoc = new GoogleSpreadsheet(
			'1k2ZGGVr1blw8QF9fxQ-80mbgThq8mcFayfaWGRoWacY'
		)
		const targetDoc = new GoogleSpreadsheet(
			'1hmp69Ej9Gr4JU1KJ6iHO-iv1Tga5lMyp8NO5-yRDMlU'
		)

		/* eslint-disable camelcase */
		await taxDoc.useServiceAccountAuth({
			client_email: sheetData.client_email,
			private_key: sheetData.private_key,
		})
		await vatRefundDoc.useServiceAccountAuth({
			client_email: sheetData.client_email,
			private_key: sheetData.private_key,
		})
		await targetDoc.useServiceAccountAuth({
			client_email: sheetData.client_email,
			private_key: sheetData.private_key,
		})
		/* eslint-enable camelcase */

		await taxDoc.loadInfo()
		await targetDoc.loadInfo()
		await vatRefundDoc.loadInfo()

		const ibpSheet = targetDoc.sheetsById['325601058'] // IBP 배송 비용
		const eldexSheet = targetDoc.sheetsById['980121221'] // 엘덱스 배송 비용
		const lotteCardSheet = targetDoc.sheetsById['1033704797'] // 롯데카드 매입내역 (정확하지만 한달에 한번씩만 갱신)
		const lotteCardExtraSheet = targetDoc.sheetsById['0'] // 롯데카드 승인내역 (부정확하지만 자주 갱신)
		const samsungCardSheet = targetDoc.sheetsById['880709363'] // 삼성카드 매출내역
		const taxSheet = taxDoc.sheetsById['1605798118']

		const eldexRaw = await eldexSheet.getRows()
		const ibpRaw = await ibpSheet.getRows()
		const lotteCardRaw = await lotteCardSheet.getRows()
		const lotteCardExtraRaw = await lotteCardExtraSheet.getRows()
		const samsungCardRaw = await samsungCardSheet.getRows()
		const taxRaw = await taxSheet.getRows()

		const eldex = {}
		const ibp = {}
		const ibpFee = {}
		const lotteCard = {}
		const lotteCardExtra = {}
		const lotteCardRefund = {}
		const samsungCard = {}
		const samsungCardRefund = {}
		const tax = {}
		const vatRefund = {}

		eldexRaw.forEach((row) => {
			const id = row['송장번호']
			const value = row['2차결제금액(원)'].replace(/,/g, '')
			const pgFee = row['PG수수료(원)'].replace(/,/g, '')
			eldex[id] = parseInt(value) + parseInt(pgFee)
		})

		ibpRaw.forEach((row) => {
			if (row['관리번호']) {
				const id = parseInt(row['관리번호'].replace(/\D/g, ''))
				let value = parseInt(row['중량'])
				if (value < 1) value = 1
				value = Math.ceil(value / 0.5)
				ibp[id] = 4.1 + 2 * value
			}
		})

		taxRaw.forEach((row) => {
			if (row['주문번호']) {
				const id = row['주문번호'].trim()
				const value = row['고지 금액'].replace(/,/g, '')
				tax[id] = parseInt(value)
			}
		})

		lotteCardRaw.forEach((row) => {
			const id = row['승인번호'].trim()
			const value = parseInt(row['승인금액'].replace(/,/g, ''))
			const isCanceled = row['매출취소 여부'] === 'Y'

			lotteCardRefund[id] = 0

			if (value < 0) {
				lotteCard[id] = -value
				lotteCardRefund[id] = value
			} else if (value > 0) {
				lotteCard[id] = value
				if (isCanceled) lotteCardRefund[id] = -value
			}
		})

		lotteCardExtraRaw.forEach((row) => {
			const id = row['승인번호'].trim()
			const value = parseInt(row['승인금액'].replace(/,/g, ''))
			const isCanceled = ['매입취소', '전액승인취소'].includes(row['승인구분'])

			if (lotteCardRefund[id] === undefined) lotteCardRefund[id] = 0

			if (value > 0) {
				lotteCardExtra[id] = value
				if (isCanceled && (lotteCardRefund[id] === 0 || lotteCardRefund[id] === undefined)) {
					lotteCardRefund[id] = -value
				}
			} else {
				lotteCardExtra[id] = -value
				if (isCanceled && (lotteCardRefund[id] === 0 || lotteCardRefund[id] === undefined)) {
					lotteCardRefund[id] = value
				}
			}
		})

		samsungCardRaw.forEach((row) => {
			const id = row['승인번호'].trim()
			const value = parseInt(row['거래금액(원화)'].replace(/,/g, ''))
			const isCanceled = row['승인취소여부'] === 'Y'

			samsungCardRefund[id] = 0

			if (value < 0) {
				samsungCard[id] = -value
				samsungCardRefund[id] = value
			} else if (value > 0) {
				samsungCard[id] = value
				if (isCanceled) samsungCardRefund[id] = -value
			}
		})

		for (const sheetId of Object.keys(vatRefundDoc.sheetsById)) {
			const sheet = vatRefundDoc.sheetsById[sheetId]

			const data = await sheet.getRows()
			data.forEach(row => {
				if (row['관리번호']) {
					const id = parseInt(row['관리번호'].replace(/\D/g, ''))
					vatRefund[id] = parseFloat(row['부가세 금액']?.replace(/[^\d.]/g, '')?.trim()) || 0
					ibpFee[id] = parseFloat(row['IBP 수수료']?.replace(/[^\d.]/g, '')?.trim()) || 0
				}
			})
		}

		const data = await MySQL.execute(
			`
          SELECT fo.created_at,
                 CONCAT(si.shop_name, ' ', sp.shop_country)                   AS shopName,
                 CONCAT(bi.brand_name, ' ', COALESCE(ion.name, ii.item_name), ' ', ii.item_code, ' (', ii.idx,
                        ')')                                                  AS itemName,
                 io.quantity                                                  AS quantity,
                 fo.fetching_order_number,
                 io.item_order_number                                         AS itemOrderNumber,

                 ccc.idx                                                      AS cardCompanyId,
                 ccc.name                                                     AS cardCompanyName,
                 io.vendor_order_number                                       AS vendorOrderNumber,
                 io.card_approval_number                                      AS cardApprovalNumber,
                 io.origin_amount                                             AS originAmount,
                 (SELECT SUM(io.origin_amount)
                  FROM commerce.shop_order so2
                           JOIN commerce.item_order io ON so2.shop_order_number = io.shop_order_number
                           LEFT JOIN commerce.order_refund_item ori
                                     ON io.item_order_number = ori.item_order_number AND ori.status = 'ACCEPT'
                  WHERE ori.item_order_number IS NULL
                    AND io.deleted_at IS NULL
                    AND so2.shop_order_number = so.shop_order_number
                    AND io.deleted_at IS NULL)                                AS shopOriginAmount,
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
                 fo.status,
                 fo.order_path,
                 fo.pay_method,
                 (SELECT JSON_ARRAYAGG(status)
                  FROM (SELECT case
                                   when ori2.status = 'ACCEPT'
                                       then '반품'
                                   when ori2.status = 'IN_PROGRESS'
                                       then '반품 진행 중'
                                   when ori2.status = 'HOLD'
                                       then '반품 보류'
                                   when oci2.cancel_item_number is not null
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
                                              oci2.status = 'ACCEPT'
                                 left join commerce.order_return_item ori2
                                           on io2.item_order_number = ori2.item_order_number AND
                                              ori2.status IN ('IN_PROGRESS', 'HOLD', 'ACCEPT')
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
                     )                                                        AS returnFee,
                 oret.reason_type                                             AS returnReason,
                 case
                     when oreti.return_item_number is not null
                         then '반품'
                     when oci.cancel_item_number is not null
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
                 fo.status                                                    AS orderStatus,
                 so.status                                                    AS shopStatus,
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
                 case
                     when oc.order_cancel_number IS NOT NULL AND (oref.refund_amount < 0 or oref.refund_amount is null)
                         then fo.pay_amount
                     else oref.refund_amount
                     end                                                      AS refundAmount,
                 fo.pay_amount_detail                                         AS payAmountDetail,
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
                  WHERE so2.fetching_order_number = fo.fetching_order_number
                    AND io2.deleted_at IS NULL)                               AS itemPayAmountDetail,
                 io.inherited_shop_coupon_discount_amount                     AS inheritedShopCouponDiscountAmount,
                 io.inherited_order_coupon_discount_amount                    AS inheritedOrderCouponDiscountAmount,
                 io.inherited_order_use_point                                 AS inheritedOrderUsePoint,
                 io.coupon_discount_amount                                    AS itemCouponDiscountAmount,
                 so.coupon_discount_amount                                    AS shopCouponDiscountAmount,
                 fo.coupon_discount_amount                                    AS orderCouponDiscountAmount,
                 fo.use_point                                                 AS orderPointDiscountAmount,
                 exists(
                         select 1
                         from commerce.order_refund refund
                         where 1 = 1
                           and refund.tax_refund_status = 'ACCEPT'
                           and refund.fetching_order_number = fo.fetching_order_number
                     )                                                        AS taxRefunded,
                 iot.total_tax                                                AS totalTax,
                 so.is_ddp_service                                            AS isDDP,
                 weight                                                       AS weight,
                 imc.idx                                                      AS ibpManageCode,
                 CONCAT(dm.name, ' ', dm.country)                             AS deliveryMethod,
                 (SELECT u.name
                  FROM commerce.fetching_order_memo fom
                           JOIN fetching_dev.users u ON fom.admin_id = u.idx
                  WHERE fom.fetching_order_number = fo.fetching_order_number
                    AND fom.to_value = 'ORDER_COMPLETE'
                  ORDER BY fom.to_value = 'ORDER_COMPLETE' DESC, fom.created_at DESC
                  LIMIT 1)                                                    AS assignee,
                 iocm.amount                                                  AS affiliateFee,
                 oci.item_order_number IS NOT NULL                            AS isCanceled,
                 oreti.item_order_number IS NOT NULL                          AS isReturned
          FROM commerce.fetching_order fo
              JOIN commerce.shop_order so
          ON fo.fetching_order_number = so.fetching_order_number
              JOIN commerce.item_order io on so.shop_order_number = io.shop_order_number
              LEFT JOIN commerce.order_cancel_item oci ON io.item_order_number = oci.item_order_number AND oci.status = 'ACCEPT'
              LEFT JOIN commerce.order_cancel oc on oci.order_cancel_number = oc.order_cancel_number
              LEFT JOIN commerce.order_return_item oreti ON oreti.item_order_number = io.item_order_number AND oreti.status = 'ACCEPT'
              LEFT JOIN commerce.order_return oret on oreti.order_return_number = oret.order_return_number
              LEFT JOIN commerce.order_return_extra_charge oretec_D ON oretec_D.order_return_number = oret.order_return_number AND oretec_D.reason_type = 'DOMESTIC_RETURN'
              LEFT JOIN commerce.order_return_extra_charge oretec_O ON oretec_O.order_return_number = oret.order_return_number AND oretec_O.reason_type = 'OVERSEAS_RETURN'
              LEFT JOIN commerce.order_return_extra_charge oretec_R ON oretec_R.order_return_number = oret.order_return_number AND oretec_R.reason_type = 'REPAIR'
              LEFT JOIN commerce.order_refund_item orefi ON orefi.item_order_number = io.item_order_number AND orefi.status = 'ACCEPT'
              LEFT JOIN commerce.order_refund oref ON orefi.order_refund_number = oref.order_refund_number
              LEFT JOIN commerce.order_delivery od ON od.fetching_order_number = fo.fetching_order_number
              LEFT JOIN commerce.shipping_company_code scc ON scc.code = io.shipping_code
              LEFT JOIN commerce.credit_card_company ccc ON ccc.idx = io.card_company_id
              JOIN commerce.user u ON fo.user_id = u.idx
              JOIN fetching_dev.delivery_method dm ON so.delivery_method = dm.idx
              LEFT JOIN commerce.item_order_tax iot ON io.item_order_number = iot.item_order_number
              LEFT JOIN commerce.item_order_weight iow ON io.item_order_number = iow.item_order_number
              LEFT JOIN commerce.ibp_manage_code imc ON io.item_order_number = imc.item_order_number
              LEFT JOIN commerce.item_order_commission_map iocm ON io.item_order_number = iocm.item_order_number
              JOIN shop_price sp ON so.shop_id = sp.idx
              JOIN fetching_dev.item_info ii ON ii.idx = io.item_id
              JOIN fetching_dev.shop_info si ON sp.shop_id = si.shop_id
              LEFT JOIN brand_info bi ON ii.brand_id = bi.brand_id
              LEFT JOIN item_original_name ion on ii.idx = ion.item_id
              LEFT JOIN shop_support_info ssi ON si.shop_id = ssi.shop_id
          WHERE fo.paid_at IS NOT NULL
            AND fo.deleted_at IS NULL
            AND io.deleted_at IS NULL
            AND (
              (fo.created_at + INTERVAL 9 HOUR) >= ?
            AND
              (fo.created_at + INTERVAL 9 HOUR)
              < ?
              )
          GROUP BY io.item_order_number
          ORDER BY fo.created_at ASC
			`,
			[start, end]
		)

		const [{currencyRate}] = await MySQL.execute(`
        SELECT currency_rate as currencyRate
        FROM currency_info
        WHERE currency_tag = 'EUR'
		`)

		const feed = data.map((row) => {
			// row.itemOrderNumber = row.itemOrderNumber.map(itemOrder => [itemOrder.itemOrderNumber, itemOrder.orderedAt ? `(${DateTime.fromISO(row.created_at.toISOString()).setZone('Asia/Seoul').toFormat('yyyy-MM-dd HH:mm:ss')})` : ''].join(' ').trim()).join(', ')
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

			const priceData: any = {}
			JSON.parse(row.payAmountDetail).forEach((row) => {
				if (priceData[row.type]) {
					priceData[row.type] += row.value
				} else {
					priceData[row.type] = row.value
				}
			})

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

			const cardApprovalNumberList =
				row.cardApprovalNumber?.split(',') ?? []

			const cardRefundValue = cardApprovalNumberList.reduce((acc, e) => {
				const cardApprovalNumber = e.trim()

				const refund = row.cardCompanyName === '삼성카드' ? samsungCardRefund[cardApprovalNumber] || 0 : lotteCardRefund[cardApprovalNumber] || 0

				return refund + acc
			}, 0)

			let cardPurchaseValue = cardApprovalNumberList.reduce((acc, e) => {
				const cardApprovalNumber = e.trim()

				const value =
					(row.cardCompanyName === '삼성카드' ? samsungCard[cardApprovalNumber] : lotteCard[cardApprovalNumber] || lotteCardExtra[cardApprovalNumber]) || 0

				return value + acc
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

			const taxTotal = row.totalTax || tax[row.itemOrderNumber] || tax[row.fetching_order_number] || 0

			const purchaseValue = itemPriceData['SHOP_PRICE_KOR'] + itemPriceData['DELIVERY_FEE'] - itemPriceData['DEDUCTED_VAT'] + (row.isDDP ? itemPriceData['DUTY_AND_TAX'] : 0) + (itemPriceData['WAYPOINT_FEE'] ?? 0)
			const lCardRefundValue = itemPriceDataCanceled['SHOP_PRICE_KOR'] + itemPriceDataCanceled['DELIVERY_FEE'] - (itemPriceData['DEDUCTED_VAT'] ?? 0) + (row.isDDP ? itemPriceDataCanceled['DUTY_AND_TAX'] : 0)

			if (row.cardApprovalNumber === '파스토') cardPurchaseValue = purchaseValue

			let waypointDeliveryFee = parseInt(eldex[row.invoice] ?? '0')

			if (row.ibpManageCode) {
				waypointDeliveryFee = Math.round((ibp[row.ibpManageCode] ?? 0) * currencyRate)
			} else if (row.weight) {
				waypointDeliveryFee = parseInt(((row.weight < 1 ? 4 + 3.2 + 1.5 : 4 * row.weight + 3.2 + 1.5) * currencyRate).toFixed(3))
			}

			let deductedVat = 0

			if (row.ibpManageCode) {
				deductedVat = Math.round(parseFloat(vatRefund[row.ibpManageCode] ?? '0') * currencyRate)
			}

			let waypointFee = 0

			if (row.ibpManageCode) {
				waypointFee = Math.round(parseFloat(ibpFee[row.ibpManageCode] ?? '0') * currencyRate)
			}

			let canceledDeductedVat = 0, canceledWaypointFee = 0

			if ((row.isCanceled && !localStatus.includes(row.status)) || row.isReturned) {
				canceledDeductedVat = deductedVat
				canceledWaypointFee = waypointFee
			}

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
				'상품 원가': cardPurchaseValue,
				'부가세 환급': deductedVat,
				'관부가세': taxTotal,
				'운송료': waypointDeliveryFee,
				'페칭 수수료': itemPriceData['FETCHING_FEE'],
				'쿠폰': coupon,
				'적립금': point,
				'PG수수료': pgFee - refundPgFee,
				'부가세 환급 수수료': waypointFee,
				'실 결제 금액': row.payAmount,
				'차액 결제 금액': row.additionalPayAmount,
				'결제 환불 금액': refundAmount,
				'관부가세 환급': refundAmount && cardPurchaseValue ? row.totalTax : 0,
				'페칭 수수료 취소': canceledFetchingFee,
				'부가세 환급 취소': canceledDeductedVat,
				'부가세 환급 수수료 취소': canceledWaypointFee,
				'쿠폰 반환': canceledCoupon,
				'적립금 반환': canceledPoint,
				'운송료 취소': 0,
				'국내 반품 비용': row.domesticExtraCharge,
				'해외 반품 비용': row.overseasExtraCharge,
				'보상 적립금': row.pointByIssue,
				'수선 비용': row.repairExtraCharge,
				'매입 환출 금액': -cardRefundValue,
				'반품 수수료': row.returnFee,
				'제휴 수수료': row.affiliateFee
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
					if (['운송료'].includes(key)) continue
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

					console.log(feed[i]['상품별 주문번호'], key, feed[i][key])
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
				fileName: '1월.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2022-01-01T00:00:00.000Z'),
					new Date('2022-02-01T00:00:00.000Z'),
					'774838589'
				),
				contentType: 'text/csv',
			})
		)

		console.log(
			await S3Client.upload({
				folderName: 'feeds',
				fileName: '2월.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2022-02-01T00:00:00.000Z'),
					new Date('2022-03-01T00:00:00.000Z'),
					'1950994764'
				),
				contentType: 'text/csv',
			})
		)

		console.log(
			await S3Client.upload({
				folderName: 'feeds',
				fileName: '3월.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2022-03-01T00:00:00.000Z'),
					new Date('2022-04-01T00:00:00.000Z'),
					'330893985'
				),
				contentType: 'text/csv',
			})
		)

		console.log(
			await S3Client.upload({
				folderName: 'feeds',
				fileName: '4월.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2022-04-01T00:00:00.000Z'),
					new Date('2022-05-01T00:00:00.000Z'),
					'2089161848'
				),
				contentType: 'text/csv',
			})
		)

		console.log(
			await S3Client.upload({
				folderName: 'feeds',
				fileName: '5월.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2022-05-01T00:00:00.000Z'),
					new Date('2022-06-01T00:00:00.000Z'),
					'1009926644'
				),
				contentType: 'text/csv',
			})
		)

		console.log(
			await S3Client.upload({
				folderName: 'feeds',
				fileName: '6월.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2022-06-01T00:00:00.000Z'),
					new Date('2022-07-01T00:00:00.000Z'),
					'1304014481'
				),
				contentType: 'text/csv',
			})
		)

		console.log(
			await S3Client.upload({
				folderName: 'feeds',
				fileName: '7월.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2022-07-01T00:00:00.000Z'),
					new Date('2022-08-01T00:00:00.000Z'),
					'437467632'
				),
				contentType: 'text/csv',
			})
		)

		console.log(
			await S3Client.upload({
				folderName: 'feeds',
				fileName: '8월.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2022-08-01T00:00:00.000Z'),
					new Date('2022-09-01T00:00:00.000Z'),
					'241586790'
				),
				contentType: 'text/csv',
			})
		)

		console.log(
			await S3Client.upload({
				folderName: 'feeds',
				fileName: '9월.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2022-09-01T00:00:00.000Z'),
					new Date('2022-10-01T00:00:00.000Z'),
					'1947643951'
				),
				contentType: 'text/csv',
			})
		)

		console.log(
			await S3Client.upload({
				folderName: 'feeds',
				fileName: '10월.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2022-10-01T00:00:00.000Z'),
					new Date('2022-11-01T00:00:00.000Z'),
					'147779322'
				),
				contentType: 'text/csv',
			})
		)

		console.log(
			await S3Client.upload({
				folderName: 'feeds',
				fileName: '11월.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2022-11-01T00:00:00.000Z'),
					new Date('2022-12-01T00:00:00.000Z'),
					'1352253972'
				),
				contentType: 'text/csv',
			})
		)

		console.log(
			await S3Client.upload({
				folderName: 'feeds',
				fileName: '12월.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2022-12-01T00:00:00.000Z'),
					new Date('2023-01-01T00:00:00.000Z'),
					'1237354903'
				),
				contentType: 'text/csv',
			})
		)
	}
}
