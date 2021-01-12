import {
	Request,
	Response,
} from 'express'
import Router from 'express-promise-router'
import { MySQL } from '../utils'
import { parse } from 'json2csv'

const router = Router()

// 네이버 쇼핑 피드
router.get('/naver-shopping', async (req: Request, res: Response) => {
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

	res.writeHead(200, {
		'Content-Disposition': 'attachment; filename="feed.tsv"',
		'Content-Type': 'text/plain',
	})
	res.end(Buffer.from(tsv, 'utf-8'))
})

// 구글 쇼핑 피드
router.get('/google-shopping', async (req: Request, res: Response) => {
	const limit = 149000
	const query = `
		SELECT
			ii.idx as 'id',
			REPLACE(ii.item_name, '\t', ' ') as 'title',
			REPLACE(ii.item_description, '\t', ' ') as 'description',
			CONCAT('https://fetching.co.kr/product/detail.html?product_no=', c24ud.product_no) as 'link',
			CONCAT('https://m.fetching.co.kr/app/detail.html?product_no=', c24ud.product_no) as 'mobile_link',
			ii.image_url as 'image_link',
			(
					SELECT SUBSTRING_INDEX(GROUP_CONCAT(REPLACE(ig.item_image_url, ',', '%2C') SEPARATOR ','), ',', 10)
					FROM item_image ig
					WHERE ig.item_id = ii.idx
					ORDER BY ig.priority ASC
			) as additional_image_link,
			(
					SELECT SUBSTRING_INDEX(GROUP_CONCAT(REPLACE(i.size_name, ',', '%2C') SEPARATOR ','), ',', 10)
					FROM item_size i
					WHERE i.item_id = ii.idx
						AND i.size_quantity > 0
			) as size,
			IF(EXISTS((
				SELECT i.item_id
				FROM item_size i
				WHERE i.item_id = ii.idx
						AND i.size_quantity > 0
				LIMIT 1
			)) > 0, 'in stock', 'out of stock') as 'availability',
			CONCAT(iop.final_price, '.00 KRW') as 'price',
			CONCAT(ip.final_price, '.00 KRW') as 'sale_price',
			fc.google_category_id as 'google_product_category',
			(
					SELECT GROUP_CONCAT(fc.fetching_category_name SEPARATOR ' > ')
					FROM item_category_map icm
					JOIN fetching_category fc on icm.fetching_category_id = fc.idx
					WHERE icm.item_id = ii.idx
					ORDER BY fc.idx ASC
					LIMIT 10
			) as product_type,
			bi.brand_name_kor 'brand',
			ii.idx as 'MPN',
			'no' as 'adult',
			IF(ii.item_gender = 'W', 'female', 'male') as 'gender',
			ii.idx as 'item_group_id'
		FROM cafe24_upload_list c24ul
		JOIN item_info ii on c24ul.item_id = ii.idx
		JOIN cafe24_upload_db c24ud on ii.idx = c24ud.item_id
		JOIN item_price ip on ii.idx = ip.item_id
		JOIN item_origin_price iop on ii.idx = iop.item_id
		JOIN brand_info bi on ii.brand_id = bi.brand_id
		JOIN fetching_category fc on (
			SELECT icm.fetching_category_id
			FROM item_category_map icm
			JOIN fetching_category fc on icm.fetching_category_id = fc.idx
			WHERE icm.item_id = ii.idx
				AND fc.google_category_id IS NOT NULL
			ORDER BY icm.fetching_category_id DESC
			LIMIT 1
		) = fc.idx
		WHERE c24ul.is_naver_upload = 1
			OR c24ul.is_smart_store_upload = 1
		LIMIT ${limit}
	`
	const data = await MySQL.execute(query)

	const tsv = parse(data, {
		fields: Object.keys(data[0]),
		delimiter: '\t',
	})

	res.writeHead(200, {
		'Content-Disposition': 'attachment; filename="feed.tsv"',
		'Content-Type': 'text/plain',
	})
	res.end(Buffer.from(tsv, 'utf-8'))
})

export default router
