import { iFeed } from './feed'
import { MySQL } from '../utils'
import { parse } from 'json2csv'
import { S3Client } from '../utils'

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
		const limit = 92000
		const query = `
			SELECT
				cud.product_no AS 'id',
				REPLACE(REPLACE(ii.item_name, '\n', ''), '\t', '') AS 'title',
				ip.final_price AS 'price_pc',
				ip.final_price AS 'price_mobile',
				iop.final_price AS 'normal_price',
				CONCAT('https://fetching.co.kr/product/detail.html?product_no=', cud.product_no) AS 'link',
				CONCAT('https://m.fetching.co.kr/app/detail.html?product_no=', cud.product_no) AS 'mobile_link',
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
				'Y' AS 'import_flag',
				'구매대행' AS 'product_flag',
				bi.brand_name AS 'brand_name',
				'100% 정품, 관부가세 포함, 기한한정 세일!' AS 'event_words',
				0 AS 'shipping',
				(
					SELECT SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(i.size_name, '^', ip.final_price + IFNULL(i.optional_price, 0)) SEPARATOR '|'), ',', 10)
					FROM item_size i
					WHERE i.item_id = ii.idx
				) AS 'option_detail',
				IF(ii.item_gender = 'M', '남성', '여성') AS 'gender',
				'Y' AS 'includes_vat',
				CONCAT_WS('|', bi.brand_name, bi.brand_name_kor, ii.item_name, IF(ii.item_gender = 'M', '남성', '여성'), (
					SELECT fc.fetching_category_name
					FROM fetching_category fc
					JOIN item_category_map icm on fc.idx = icm.fetching_category_id
					WHERE icm.item_id = ii.idx
					ORDER BY icm.fetching_category_id DESC
					LIMIT 1
				)) AS 'search_tag'
			FROM item_info ii
			JOIN brand_info bi on ii.brand_id = bi.brand_id
			JOIN item_price ip on ii.idx = ip.item_id
			JOIN item_origin_price iop on ii.idx = iop.item_id
			JOIN cafe24_upload_db cud on ii.idx = cud.item_id
			WHERE ii.is_verify = 1
				AND cud.is_active = 1
			LIMIT ${limit}
		`
		const data = await MySQL.execute(query)

		const tsv = parse(data, {
			fields: Object.keys(data[0]),
			delimiter: '\t',
		})
	
		return Buffer.from(tsv, 'utf-8')
	}
}

