import { iFeed } from './feed'
import { MySQL, S3Client } from '../utils'
import {parse} from 'json2csv'
import moment from 'moment'


export class NaverSalesFeed implements iFeed {
	async upload() {
		const buffer = await this.getTsvBuffer()

		const feedUrl = await S3Client.upload({
			folderName: 'feeds',
			fileName: 'naver-sales-feed.tsv',
			buffer,
		})
		console.log(`FEED_URL: ${feedUrl}`)
	}

	async getTsvBuffer(): Promise<Buffer> {
		return Buffer.from(await this.getTsv(), 'utf-8')
	}

	async getTsv() {
		const limit = 100000
		const targetDay = new Date()
		targetDay.setDate(targetDay.getDate() - 1)
		const nextDay = new Date()
		const target = moment(targetDay).format('YYYY-MM-DD')
		const targetEnd = moment(nextDay).format('YYYY-MM-DD')

		const query = `
			SELECT DISTINCT ii.idx AS mall_id,
			                IF(inflowTotal < orderTotal, inflowTotal, orderTotal) AS sale_count,
			                IFNULL(amount, 0) AS sale_price,
			                IF(inflowTotal < orderTotal, inflowTotal, orderTotal) AS order_count,
			                ? AS dt
			FROM naver_upload_list nul
			    JOIN item_info ii on nul.item_id = ii.idx
			    JOIN (
			        SELECT i.itemId, COUNT(*) inflowTotal, orderTotal, amount, CAST(date AS DATE)
			        FROM fetching_logs.inflow i
			            JOIN (
			                SELECT itemId, COUNT(*) orderTotal, sum(amount) amount
			                FROM fetching_logs.\`order\`
			                WHERE date > ? AND date < ?
			                  AND isValid != 0
			                GROUP BY itemId
			            ) o ON i.itemId = o.itemId
			            JOIN item_info ii ON ii.idx = i.itemId
			        WHERE i.\`from\` = 'NAVER'
			          AND date > ? AND date < ?
			          AND i.itemId != 0
			        GROUP BY i.itemId
			    ) i ON i.itemId = ii.idx
			    JOIN shop_info si ON ii.shop_id = si.shop_id
			    JOIN item_show_price isp on ii.idx = isp.item_id
			    JOIN item_price ip on ii.idx = ip.item_id AND isp.price_rule = ip.price_rule
			    JOIN brand_info bi ON ii.brand_id = bi.brand_id
			    JOIN item_list_for_update vu ON ii.idx = vu.item_id
			    JOIN item_category_map icm ON ii.idx = icm.item_id
			    JOIN fetching_category fc ON icm.fetching_category_id = fc.idx
			    LEFT JOIN item_naver_product_id inpi on ii.idx = inpi.idx
			WHERE fc.fetching_category_depth = 2
			AND ii.is_sellable
			ORDER BY nul.sequence
			LIMIT ?
		`
		let data = await MySQL.execute(query, [
			target, target, targetEnd, target, targetEnd, limit
		])

		data = data.map((row) => {
			/* eslint-disable camelcase */
			row.mall_id = `F${row.mall_id}`
			if (!row.sale_count) row.sale_count = 0
			if (!row.order_count) row.order_count = 0
			return row
			/* eslint-enable camelcase */
		})

		return parse(data, {
			fields: Object.keys(data[0]),
			delimiter: '\t',
			quote: '',
		})
	}
}

