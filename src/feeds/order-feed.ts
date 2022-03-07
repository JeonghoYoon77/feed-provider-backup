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
             fo.pay_amount,
             fo.status,
             fo.order_path,
             fo.pay_method,
             oc.order_cancel_number IS NOT NULL AS isCanceled,
             oe.order_exchange_number IS NOT NULL AS isExchanged,
             oret.order_return_number IS NOT NULL AS isReturned,
						 oref.refund_amount AS refundAmount,
						 JSON_ARRAYAGG(io.pay_amount_detail) AS payAmountDetail
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
			row.payAmountDetail.map(detail => Object.values<any>(JSON.parse(detail)).forEach(data => {
				data.forEach(row => {
					if (priceData[row.type]) priceData[row.type] += row.rawValue
					else priceData[row.type] = row.rawValue
				})
			}))

			const deposit = row.pay_method === 'DEPOSIT' ? row.pay_amount : 0
			const card = row.pay_method === 'CARD' ? row.pay_amount : 0
			const kakao = row.pay_method === 'KAKAO' ? row.pay_amount : 0
			const naver = row.pay_method === 'NAVER' ? row.pay_amount : 0
			const escrow = row.pay_method === 'ESCROW' ? row.pay_amount : 0
			const escrowCard = row.pay_method === 'ESCROW_CARD' ? row.pay_amount : 0

			const refundAmount = row.refundAmount ?? 0
			const salesAmount = row.pay_amount - refundAmount
			const totalPrice = priceData.SHOP_PRICE_KOR + priceData.DUTY_AND_TAX + priceData.DELIVERY_FEE
			const pgFee = card * 0.014 + kakao * 0.015 + naver * 0.015 + escrow * 0.017 + escrowCard * 0.016
			const totalTotalPrice = !row.refundAmount ? (totalPrice + pgFee - refundAmount) : 0

			const profit = salesAmount - totalTotalPrice

			return {
				createdAt: row.created_at,
				completedAt: row.completed_at,
				userName: row.name,
				itemName: row.itemName,
				fetchingOrderNumber: row.fetching_order_number,
				amount: row.pay_amount,
				status: null,
				orderPath: row.order_path,
				payMethod: row.pay_method,
				isCanceled: row.isCanceled,
				isExchanged: row.isExchanged,
				isReturned: row.isReturned,
				deposit,
				card,
				kakao,
				naver,
				escrow,
				escrowCard,
				returns: refundAmount,
				salesAmount,
				totalTotalPrice,
				cardApprovalNumber: '',
				totalPrice,
				pgFee,
				eldex: priceData.ADDITIONAL_FEE,
				dutyAndTax: priceData.DUTY_AND_TAX,
				purchaseReturn: 0,
				pgFeeRefund: row.refundAmount ? pgFee : 0,
				subtractedFee: 0,
				profit
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
