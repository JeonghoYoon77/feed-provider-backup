import { iFeed } from './feed'
import { MySQL } from '../utils'
import { parse } from 'json2csv'
import { S3Client } from '../utils'

const LIMIT = 99999

export class NaverFeed implements iFeed {
	async upload() {
		const buffer = await this.getTsvBuffer()

		const feedUrl = await S3Client.upload({
			folderName: 'feeds',
			fileName: 'naver-feed.tsv',
			buffer,
		})
		console.log(`FEED_URL: ${feedUrl}`)
	}

	async getTsvBuffer(): Promise<Buffer> {
		return Buffer.from(await this.getTsv(), 'utf-8')
	}

	async getTsv() {
		const query = `
			SELECT
				cud.product_no AS 'id',
				REPLACE(REPLACE(CONCAT_WS(' ', bi.main_name, IF(ii.item_gender = 'W', '여성', '남성'), fc.fetching_category_name, ii.item_name, ii.color), '\n', ''), '\t', '') AS 'title',
				ip.final_price AS 'price_pc',
				ip.final_price AS 'price_mobile',
				iop.final_price AS 'normal_price',
				CONCAT('https://fetching.co.kr/product/detail.html?product_no=', cud.product_no) AS 'link',
				CONCAT('https://m.fetching.co.kr/product/detail.html?product_no=', cud.product_no) AS 'mobile_link',
				ii.image_url AS 'image_link',
				(
					SELECT SUBSTRING_INDEX(GROUP_CONCAT(REPLACE(ig.item_image_url, ',', '%2C') SEPARATOR ','), ',', 10)
					FROM item_image ig
					WHERE ig.item_id = ii.idx
					ORDER BY ig.priority ASC
				) AS 'add_image_link',
				(
					SELECT fc.fetching_category_name
					FROM fetching_category fc
					JOIN item_category_map icm on fc.idx = icm.fetching_category_id
					WHERE icm.item_id = ii.idx
						AND fc.fetching_category_depth = 0
					LIMIT 1
				) AS 'category_name1',
				(
					SELECT fc.fetching_category_name
					FROM fetching_category fc
					JOIN item_category_map icm on fc.idx = icm.fetching_category_id
					WHERE icm.item_id = ii.idx
						AND fc.fetching_category_depth = 1
					LIMIT 1
				) AS 'category_name2',
				(
					SELECT fc.fetching_category_name
					FROM fetching_category fc
					JOIN item_category_map icm on fc.idx = icm.fetching_category_id
					WHERE icm.item_id = ii.idx
						AND fc.fetching_category_depth = 2
					LIMIT 1
				) AS 'category_name3',
				(
					SELECT fc.smartstore_category_id
					FROM fetching_category fc
					JOIN item_category_map icm on fc.idx = icm.fetching_category_id
					WHERE icm.item_id = ii.idx
					ORDER BY icm.fetching_category_id DESC
					LIMIT 1
				) AS 'naver_category',
				'신상품' AS 'condition',
				bi.main_name AS 'brand_name',
				'100% 정품, 관부가세 포함, 기한한정 세일!' AS 'event_words',
				0 AS 'shipping',
				(
					SELECT SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(i.size_name, '^', CEIL((ip.final_price + IFNULL(i.optional_price, 0)) * 0.97 / 100) * 100) SEPARATOR '|'), ',', 10)
					FROM item_size i
					WHERE i.item_id = ii.idx
				) AS 'option_detail',
				IF(ii.item_gender = 'M', '남성', '여성') AS 'gender',
				'Y' AS 'includes_vat',
				REPLACE(CONCAT_WS('|',
					CONCAT_WS(' ', IF(ii.item_gender = 'W', '여성', '남성'), '명품', fc.fetching_category_name),
					CONCAT_WS(' ', IF(ii.item_gender = 'W', '여성', '남성'), bi.main_name, fc.fetching_category_name),
					(
						SELECT bsi.semi_name
						FROM brand_semi_name bsi
						WHERE bsi.brand_id = bi.brand_id
						LIMIT 1
					),
					(
						SELECT bsi.semi_name
						FROM brand_semi_name bsi
						WHERE bsi.brand_id = bi.brand_id
						LIMIT 1
						OFFSET 1
					),
					(
						SELECT bsi.semi_name
						FROM brand_semi_name bsi
						WHERE bsi.brand_id = bi.brand_id
						LIMIT 1
						OFFSET 2
					)
				), '\t', ' ') AS 'search_tag'
			FROM cafe24_upload_list cul
			JOIN cafe24_upload_db cud on cul.item_id = cud.item_id
			JOIN item_info ii on cud.item_id = ii.idx
            JOIN shop_info si on ii.shop_id = si.shop_id
			JOIN brand_info bi on ii.brand_id = bi.brand_id
			JOIN item_price ip on ii.idx = ip.item_id
			JOIN item_origin_price iop on ii.idx = iop.item_id
			JOIN fetching_category fc on (
				SELECT icm.fetching_category_id
				FROM fetching_category fc
				JOIN item_category_map icm on fc.idx = icm.fetching_category_id
				WHERE icm.item_id = ii.idx
					AND fc.fetching_category_name != '기타'
				ORDER BY fc.idx DESC
				LIMIT 1
			) = fc.idx
            WHERE ii.is_verify = 1
              AND cul.is_naver_upload = 1
			ORDER BY (
				# 상품 우선 순위
				(ii.item_priority > 0) * 1000
				# 프로모션 우선 순위
				+ if(exists((
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
			LIMIT ${LIMIT}
		`
		const data = await MySQL.execute(query)

		return parse(data, {
			fields: Object.keys(data[0]),
			delimiter: '\t',
			quote: '',
		})
	}
}

