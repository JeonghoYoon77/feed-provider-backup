import {iFeed} from './feed'
import {MySQL, S3Client} from '../utils'
import {parse} from 'json2csv'

export class OrderFeed implements iFeed {
	async getTsvBuffer(): Promise<Buffer> {
		return Buffer.from(await this.getTsv(), 'utf-8')
	}

	async getTsv(): Promise<string> {
		const data = await MySQL.execute(`
		  SELECT fo.created_at,
             null as completed_at,
             u.name,
						 GROUP_CONCAT(DISTINCT ii.item_name) AS itemName,
             fo.fetching_order_number,
						 so.vendor_order_number,
						 so.card_approval_number,
             fo.pay_amount,
             fo.status,
             fo.order_path,
             fo.pay_method,
             oc.order_cancel_number IS NOT NULL AS isCanceled,
             oe.order_exchange_number IS NOT NULL AS isExchanged,
             oret.order_return_number IS NOT NULL AS isReturned,
						 oref.refund_amount AS refundAmount,
						 fo.pay_amount_detail AS payAmountDetail
      FROM commerce.fetching_order fo
               LEFT JOIN commerce.order_cancel oc on fo.fetching_order_number = oc.fetching_order_number
               LEFT JOIN commerce.order_exchange oe on fo.fetching_order_number = oe.fetching_order_number
               LEFT JOIN commerce.order_return oret on fo.fetching_order_number = oret.fetching_order_number
							 LEFT JOIN commerce.order_refund oref on fo.fetching_order_number = oref.fetching_order_number
               JOIN commerce.user u on fo.user_id = u.idx
               JOIN commerce.shop_order so ON fo.fetching_order_number = so.fetching_order_number
               JOIN commerce.item_order io on so.shop_order_number = io.shop_order_number
               JOIN fetching_dev.item_info ii ON ii.idx = io.item_id
			WHERE fo.paid_at IS NOT NULL AND fo.deleted_at IS NULL
			GROUP BY fo.fetching_order_number
			ORDER BY fo.created_at ASC
		`)

		const feed = data.map(row => {
			const priceData: any = {}
			JSON.parse(row.payAmountDetail).forEach(row => {
				if (priceData[row.type]) priceData[row.type] += row.value
				else priceData[row.type] = row.value
			})

			let pgFee = 0

			if (row.pay_method === 'CARD') pgFee = row.pay_amount * 0.014
			else if (row.pay_method === 'KAKAO') pgFee = row.pay_amount * 0.015
			else if (row.pay_method === 'NAVER') pgFee = row.pay_amount * 0.015
			else if (row.pay_method === 'ESCROW') pgFee = row.pay_amount * 0.017
			else if (row.pay_method === 'ESCROW_CARD') pgFee = row.pay_amount * 0.016

			const refundAmount = row.refundAmount ?? 0
			// const salesAmount = row.pay_amount - refundAmount
			// const totalPrice = priceData.SHOP_PRICE_KOR + priceData.DUTY_AND_TAX + priceData.DELIVERY_FEE
			// const totalTotalPrice = !row.refundAmount ? (totalPrice + pgFee - refundAmount) : 0

			// const profit = salesAmount - totalTotalPrice

			return {
				'주문일': row.created_at,
				'구매확정일': row.completed_at,
				'주문자': row.name,
				'상품명': row.itemName,
				'주문번호': row.fetching_order_number,
				'편집샵 주문번호': row.vendor_order_number,
				'카드 승인번호': row.card_approval_number,
				'결제 방식': row.pay_method,
				'페칭 판매가': priceData['ORIGIN_PRICE'],
				'쿠폰': priceData['COUPON_DISCOUNT'] ?? 0,
				'적립금': priceData['POINT_DISCOUNT'] ?? 0,
				'결제가': row.pay_amount,
				'환불금액': refundAmount,
				'PG수수료': pgFee,
				'엘덱스 비용': priceData.ADDITIONAL_FEE,
				'관부가세': priceData.DUTY_AND_TAX,
				'PG수수료 환불': row.refundAmount ? pgFee : 0,
				'매입환출': row.refundAmount,
				'매입환출 완료여부': 'N',
				'반품수수료': 0
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
			contentType: 'text/csv'
		})

		console.log(`FEED_URL: ${feedUrl}`)
	}
}
