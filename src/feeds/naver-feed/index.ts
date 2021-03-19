import { parse } from 'json2csv'
import { format } from 'mysql2'

import { iFeed } from '../feed'
import { MySQL } from '../../utils'
import { S3Client } from '../../utils'
import Constants from './constants'
import TSVFormat from './tsv-format'
import TSVData from './tsv-data'

const constants = new Constants()

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

	async getTsv(): Promise<string> {
		const data = await MySQL.execute(NaverFeed.query())
		const tsvData: TSVData[] = data.map(NaverFeed.makeRow)

		return parse(tsvData, {
			fields: Object.keys(tsvData[0]),
			delimiter: '\t',
			quote: '',
		})
	}

	private static query(): string {
		return format(`
			SELECT
				cud.product_no AS 'id',
				
				bi.main_name,
				ii.item_gender,
				fc.fetching_category_name,
				ii.item_name,
				ii.custom_color,
				
				ip.final_price AS 'ip_final_price',
				iop.final_price AS 'iop_final_price',

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
				(
					SELECT SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(i.size_name, '^', CEIL((ip.final_price + IFNULL(i.optional_price, 0)) * 0.97 / 100) * 100) SEPARATOR '|'), ',', 10)
					FROM item_size i
					WHERE i.item_id = ii.idx
				) AS 'option_detail',
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
				), '\t', ' ') AS 'search_tag',
				IF(iif.item_id IS NULL, 'Y', 'N') AS import_flag
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
			LEFT JOIN item_import_flag iif ON iif.item_id = ii.idx 
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
			LIMIT ?
		`, [constants.limit()])
	}

	private static makeRow(row): TSVData {
		const tsvFormat = new TSVFormat({
			itemGender: row.item_gender,
			id: row.id,
		})
		const title: string = tsvFormat.title({
			mainName: row.main_name,
			fetchingCategoryName: row.fetching_category_name,
			itemName: row.item_name,
			customColor: row.custom_color,
		})
		const pcLink: string = tsvFormat.pcLink({
			cafe24PCAddress: constants.cafe24PCAddress(),
		})
		const mobileLink: string = tsvFormat.mobileLink({
			cafe24MobileAddress: constants.cafe24MobileAddress(),
		})

		return {
			id: row.id,
			title,
			'price_pc': row.ip_final_price,
			'price_mobile': row.ip_final_price,
			'normal_price': row.iop_final_price,
			link: pcLink,
			'mobile_link': mobileLink,
			'image_link': row.image_link,
			'add_image_link': row.add_image_link,
			'category_name1': row.category_name1,
			'category_name2': row.category_name2,
			'category_name3': row.category_name3,
			'naver_category': row.naver_category,
			condition: constants.condition(),
			'brand_name': row.main_name,
			'event_words': constants.eventWords(),
			shipping: constants.shipping(),
			'import_flag': constants.importFlag(),
			'option_detail': row.option_detail,
			gender: tsvFormat.gender(),
			'includes_vat': constants.includesVat(),
			'search_tag': row.search_tag,
		}
	}
}

