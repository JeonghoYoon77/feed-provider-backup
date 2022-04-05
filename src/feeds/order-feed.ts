import { GoogleSpreadsheet } from 'google-spreadsheet'
import { parse } from 'json2csv'

import sheetData from '../../fetching-sheet.json'

import { iFeed } from './feed'
import { MySQL, S3Client } from '../utils'
import { decryptInfo } from '../utils/privacy-encryption'

export class OrderFeed implements iFeed {
	async getTsvBuffer(): Promise<Buffer> {
		return Buffer.from(await this.getTsv(), 'utf-8')
	}

	async getTsv(): Promise<string> {
		const doc = new GoogleSpreadsheet(
			'1eb3BblL771lOxbO-tK44ULTA_Llg8oPDlAoFz2xVet8',
		)
		const taxDoc = new GoogleSpreadsheet(
			'1SoZM_RUVsuIMyuJdOzWYmwirSb-2c0-5peEPm9K0ATU',
		)

		/* eslint-disable camelcase */
		await doc.useServiceAccountAuth({
			client_email: sheetData.client_email,
			private_key: sheetData.private_key,
		})
		await taxDoc.useServiceAccountAuth({
			client_email: sheetData.client_email,
			private_key: sheetData.private_key,
		})
		/* eslint-enable camelcase */

		await doc.loadInfo()
		await taxDoc.loadInfo()

		const eldexSheet = doc.sheetsById['1342024732']
		const cardSheet = doc.sheetsById['895507072']
		const taxSheet = taxDoc.sheetsById['1605798118']

		const eldexRaw = await eldexSheet.getRows()
		const cardRaw = await cardSheet.getRows()
		const taxRaw = await taxSheet.getRows()

		const eldex = {}
		const card = {}
		const cardRefund = {}
		const tax = {}

		eldexRaw.map((row) => {
			const id = row['송장번호']
			const value = row['2차결제금액(원)'].replace(/,/g, '')
			eldex[id] = parseInt(value)
		})

		taxRaw.map((row) => {
			const id = row['주문번호'].trim()
			const value = row['금액'].replace(/,/g, '')
			tax[id] = parseInt(value)
		})

		cardRaw.map((row) => {
			const id = row['승인번호'].trim()
			const value = parseInt(row['청구금액'].replace(/,/g, ''))
			const refundValue = parseInt(
				row['전월취소및부분취소'].replace(/,/g, ''),
			)
			cardRefund[id] = refundValue
			if (value > 0) card[id] = value
			else cardRefund[id] += value
		})

		const data = await MySQL.execute(`
			SELECT fo.created_at,
						 null                                 as completed_at,
						 od.recipient_name                    as name,
						 od.phone_number                      as phone,
						 si.shop_name,
						 GROUP_CONCAT(DISTINCT ii.item_name)  AS itemName,
						 fo.fetching_order_number,
						 GROUP_CONCAT(DISTINCT so.vendor_order_number) AS vendorOrderNumber,
						 GROUP_CONCAT(DISTINCT so.card_approval_number) AS cardApprovalNumber,
						 fo.pay_amount                        AS payAmount,
						 (
							 SELECT JSON_ARRAYAGG(JSON_OBJECT('method', oapi.pay_method, 'amount', oapi.amount))
							 FROM commerce.order_additional_pay oap
							     JOIN commerce.order_additional_pay_item oapi ON oapi.order_additional_number = oap.order_additional_number AND oapi.status = 'PAID'
							 WHERE oap.fetching_order_number = fo.fetching_order_number
						 )                                    AS additionalPayInfo,
						 fo.status,
						 fo.order_path,
						 fo.pay_method,
						 oc.order_cancel_number IS NOT NULL   AS isCanceled,
						 oe.order_exchange_number IS NOT NULL AS isExchanged,
						 oret.order_return_number IS NOT NULL AS isReturned,
			       ssi.customer_negligence_return_fee   AS returnFee,
			       oret.reason_type                     AS returnReason,
						 fo.status                            AS orderStatus,
						 so.status                            AS shopStatus,
						 io.invoice                           AS invoice,
						 oref.refund_amount                   AS refundAmount,
						 fo.pay_amount_detail                 AS payAmountDetail,
						 JSON_ARRAYAGG(io.pay_amount_detail)  AS itemPayAmountDetail
			FROM commerce.fetching_order fo
						 LEFT JOIN commerce.order_cancel oc on fo.fetching_order_number = oc.fetching_order_number
						 LEFT JOIN commerce.order_exchange oe on fo.fetching_order_number = oe.fetching_order_number
						 LEFT JOIN commerce.order_return oret on fo.fetching_order_number = oret.fetching_order_number
						 LEFT JOIN commerce.order_refund oref on fo.fetching_order_number = oref.fetching_order_number
						 LEFT JOIN commerce.order_delivery od ON od.fetching_order_number = fo.fetching_order_number
						 JOIN commerce.user u on fo.user_id = u.idx
						 JOIN commerce.shop_order so ON fo.fetching_order_number = so.fetching_order_number
						 JOIN commerce.item_order io on so.shop_order_number = io.shop_order_number
						 JOIN fetching_dev.item_info ii ON ii.idx = io.item_id
						 JOIN fetching_dev.shop_info si ON ii.shop_id = si.shop_id
						 JOIN shop_support_info ssi ON si.shop_id = ssi.shop_id
			WHERE fo.paid_at IS NOT NULL
				AND fo.deleted_at IS NULL
			GROUP BY fo.fetching_order_number
			ORDER BY fo.created_at ASC
		`)

		const feed = data.map((row) => {
			const priceData: any = {}
			JSON.parse(row.payAmountDetail).forEach((row) => {
				if (priceData[row.type]) priceData[row.type] += row.value
				else priceData[row.type] = row.value
			})

			let pgFee = 0

			if (row.pay_method === 'CARD')
				pgFee = Math.round(row.payAmount * 0.014)
			else if (row.pay_method === 'KAKAO')
				pgFee = Math.round(row.payAmount * 0.015)
			else if (row.pay_method === 'NAVER')
				pgFee = Math.round(row.payAmount * 0.015)
			else if (row.pay_method === 'ESCROW')
				pgFee = Math.round(row.payAmount * 0.017)
			else if (row.pay_method === 'ESCROW_CARD')
				pgFee = Math.round(row.payAmount * 0.016)

			if (row.additionalPayInfo) {
				for (const { amount, method } of row.additionalPayInfo) {
					row.payAmount += amount
					if (method === 'CARD') pgFee += Math.round(amount * 0.014)
					else if (method === 'KAKAO')
						pgFee += Math.round(amount * 0.015)
					else if (method === 'NAVER')
						pgFee += Math.round(amount * 0.015)
					else if (method === 'ESCROW')
						pgFee += Math.round(amount * 0.017)
					else if (method === 'ESCROW_CARD')
						pgFee += Math.round(amount * 0.016)
				}
			}

			const refundAmount = row.refundAmount ?? 0
			// const salesAmount = row.payAmount - refundAmount
			// const totalPrice = priceData.SHOP_PRICE_KOR + priceData.DUTY_AND_TAX + priceData.DELIVERY_FEE
			// const totalTotalPrice = !row.refundAmount ? (totalPrice + pgFee - refundAmount) : 0

			// const profit = salesAmount - totalTotalPrice

			let status = ''

			if (row.isExchanged) {
				status = '교환'
			} else if (row.isReturned) {
				status = '반품'
			} else if (row.isCanceled) {
				status = '주문 취소'
			} else {
				if (
					![
						'BEFORE_DEPOSIT',
						'ORDER_AVAILABLE',
						'ORDER_WAITING',
						'PRE_ORDER_REQUIRED',
						'ORDER_DELAY',
					].includes(row.shopStatus)
				) {
					status = '주문 완료'
				}
				if (row.orderStatus === 'COMPLETE') {
					status = '구매 확정'
				}
			}

			const cardApprovalNumber = row.card_approval_number?.trim()
			const cardRefundValue = cardRefund[cardApprovalNumber] || 0

			const itemPriceData: any = {}
			row.itemPayAmountDetail.map((detail) =>
				Object.values<any>(JSON.parse(detail)).forEach((data) => {
					data.forEach((row) => {
						if (itemPriceData[row.type])
							itemPriceData[row.type] += row.rawValue
						else itemPriceData[row.type] = row.rawValue
					})
				}),
			)

			if (
				['DEFECTIVE_PRODUCT', 'WRONG_DELIVERY'].includes(
					row.returnReason,
				) ||
				!row.isReturned
			) {
				row.returnFee = 0
			}

			if (row.fetching_order_number == '20220122-0000011') {
				console.log(card[cardApprovalNumber])
			}

			return {
				주문일: row.created_at,
				구매확정일: row.completed_at,
				상태: status,
				주문자: row.name,
				전화번호: decryptInfo(row.phone),
				편집샵명: row.shop_name,
				상품명: row.itemName,
				주문번호: row.fetching_order_number,
				'편집샵 주문번호': row.vendorOrderNumber,
				'카드 승인번호': row.cardApprovalNumber,
				'결제 방식': row.pay_method,
				'페칭 판매가': priceData['ORIGIN_PRICE'],
				'페칭 수수료': itemPriceData['FETCHING_FEE'],
				쿠폰: priceData['COUPON_DISCOUNT'] ?? 0,
				적립금: priceData['POINT_DISCOUNT'] ?? 0,
				결제가: row.payAmount,
				환불금액: refundAmount,
				PG수수료: pgFee,
				'엘덱스 비용': eldex[row.invoice] || 0,
				관부가세: tax[row.fetching_order_number] || 0,
				'PG수수료 환불': row.refundAmount ? pgFee : 0,
				'매입 금액': card[cardApprovalNumber] || 0,
				'실 매입환출금액': cardRefundValue,
				'매입환출 완료여부': cardRefundValue !== 0 ? 'Y' : 'N',
				반품수수료: row.returnFee || 0,
			}
		})

		return parse(feed, {
			fields: Object.keys(feed[0]),
			delimiter: ',',
			quote: '"',
		})
	}

	async upload() {
		const buffer = await this.getTsvBuffer()

		const feedUrl = await S3Client.upload({
			folderName: 'feeds',
			fileName: 'order-feed.csv',
			buffer,
			contentType: 'text/csv',
		})

		console.log(`FEED_URL: ${feedUrl}`)
	}
}
