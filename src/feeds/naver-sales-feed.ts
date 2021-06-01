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
			SELECT DISTINCT cud.product_no AS mall_id,
			                IF(inflowTotal < orderTotal, inflowTotal, orderTotal) AS sales_count,
			                IFNULL(amount, 0) AS sale_price,
			                IF(inflowTotal < orderTotal, inflowTotal, orderTotal) AS order_count,
			                ? AS dt
			FROM cafe24_upload_list cul
			    JOIN cafe24_upload_db cud on cul.item_id = cud.item_id
			    JOIN item_info ii on cud.item_id = ii.idx
			    LEFT JOIN (
			        SELECT i.itemId, COUNT(*) inflowTotal, orderTotal, amount, CAST(date AS DATE)
			        FROM fetching_logs.inflow i
			            LEFT JOIN (
			                SELECT itemId, COUNT(*) orderTotal, sum(amount) amount
			                FROM fetching_logs.\`order\`
			                WHERE date > ? AND date < ?
			                  AND isRefunded = 0
			                GROUP BY itemId
			            ) o ON i.itemId = o.itemId
			            JOIN item_info ii ON ii.idx = i.itemId
			            JOIN cafe24_upload_list cul on ii.idx = cul.item_id
			            JOIN cafe24_upload_db cud on cul.item_id = cud.item_id
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
			AND cul.is_naver_upload = 1
			AND cul.is_valid = 1
			AND ii.is_verify
			ORDER BY ii.item_priority > 0 DESC, # 상품 우선 순위
						 inpi.naver_product_id IS NOT NULL DESC, # 네이버 가격비교 연결 상태
						 (
								 # 프로모션 우선 순위
								 if(exists((
										 SELECT spm.item_id
												 FROM shop_promotion_map spm
														 JOIN shop_promotions sp on spm.shop_promotion_id = sp.id
												 WHERE spm.item_id = ii.idx
													 AND (
															 (CURRENT_TIMESTAMP > sp.started_at OR sp.started_at is NULL)
																	 AND
															 (CURRENT_TIMESTAMP < sp.ended_at OR sp.ended_at is NULL)
													 )
													 AND sp.is_active
												 LIMIT 1
								 )), 5, 0)
								 # 자체 할인 우선 순위
								 + if(ip.discount_rate >= 0.1, 1, 0)
								 # 카테고리 우선 순위
								 + fc.priority
								 # 카테고리 별 주요 브랜드 우선순위
								 + if(ii.idx IN (
										 SELECT ii.idx
										 FROM item_info as ii
												 JOIN item_category_map icm ON ii.idx = icm.item_id
												 JOIN important_brands_of_fetching_categories fcib
														 ON icm.fetching_category_id = fcib.category_id AND
																ii.brand_id = fcib.brand_id
												 JOIN item_price ip ON ii.idx = ip.item_id AND ip.fixed_rate > 0
								 ), 1, 0)
								 # 브랜드 우선 순위
								 + if(ii.idx IN (
										 SELECT DISTINCT (ii.idx) AS idx
										 FROM item_info AS ii,
													item_category_map AS icm,
													important_brands_of_fetching_categories AS fcib
										 WHERE ii.idx = icm.item_id
											 AND icm.fetching_category_id = fcib.category_id
											 AND fcib.brand_id = ii.brand_id
								 ), 1, 0)
						 ) DESC,
						 si.priority DESC
			LIMIT ?
		`
		const data = await MySQL.execute(query, [
			target, target, targetEnd, target, targetEnd, limit
		])

		data.map((row) => {
			/* eslint-disable camelcase */
			if (!row.sales_count) row.sales_count = 0
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

