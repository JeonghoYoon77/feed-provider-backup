import { iFeed } from './feed'
import { MySQL } from '../utils'
import { parse } from 'json2csv'
import { S3Client } from '../utils'

export class GoogleFeed implements iFeed {
	async upload() {
		const buffer = await this.getTsvBuffer()

		const feedUrl = await S3Client.upload({
			folderName: 'feeds',
			fileName: 'google-feed.tsv',
			buffer,
		})
		console.log(`FEED_URL: ${feedUrl}`)
	}

	async getTsvBuffer(): Promise<Buffer> {
		const limit = 149000
		const query = `
			SELECT
				ii.idx as 'id',
				REPLACE(ii.item_name, '\t', ' ') as 'title',
				REPLACE(ii.item_description, '\t', ' ') as 'description',
				IF(c24ud.product_no,
				   CONCAT('https://fetching.co.kr/product/detail.html?product_no=', c24ud.product_no),
				   CONCAT('https://fetching.co.kr/product_detail_app.html?product_no=', ii.idx)
				) as 'link',
				IF(c24ud.product_no,
				   CONCAT('https://m.fetching.co.kr/product/detail.html?product_no=', c24ud.product_no),
				   CONCAT('https://m.fetching.co.kr/product_detail_app.html?product_no=', ii.idx)
				) as 'mobile_link',
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
				CONCAT(IF(c24ud.product_no, iop.final_price, iop.total_price), '.00 KRW') as 'price',
				CONCAT(IF(c24ud.product_no, ip.final_price, iup.total_price), '.00 KRW') as 'sale_price',
				fc.google_category_id as 'google_product_category',
				(
						SELECT GROUP_CONCAT(fc.fetching_category_name SEPARATOR ' > ')
						FROM item_category_map icm
						JOIN fetching_category fc on icm.fetching_category_id = fc.idx
						WHERE icm.item_id = ii.idx
						ORDER BY fc.idx ASC
						LIMIT 10
				) as product_type,
				bi.brand_name_kor as 'brand',
				ii.idx as 'MPN',
				'no' as 'adult',
				IF(ii.item_gender = 'W', 'female', 'male') as 'gender',
				ii.idx as 'item_group_id'
			FROM item_info ii
			LEFT JOIN naver_upload_list nul on nul.item_id = ii.idx
			LEFT JOIN cafe24_upload_db c24ud on ii.idx = c24ud.item_id
			JOIN item_show_price isp on ii.idx = isp.item_id
			JOIN item_price ip on ii.idx = ip.item_id AND isp.price_rule = ip.price_rule
			JOIN item_user_price iup on ii.idx = iup.item_id
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
			WHERE ii.is_sellable
			ORDER BY nul.sequence
			LIMIT ${limit}
		`
		let data = await MySQL.execute(query)

		data = data.map(row => {
			const link = new URL(row.link)
			link.searchParams.set('utm_source', 'google')
			link.searchParams.set('utm_medium', 'cpc')
			link.searchParams.set('utm_campaign', 'gfeed')
			row.link = link.toString()

			const mobileLink = new URL(row.mobile_link)
			mobileLink.searchParams.set('utm_source', 'google')
			mobileLink.searchParams.set('utm_medium', 'cpc')
			mobileLink.searchParams.set('utm_campaign', 'gfeed')
			// eslint-disable-next-line camelcase
			row.mobile_link = mobileLink.toString()
		})

		const tsv = parse(data, {
			fields: Object.keys(data[0]),
			delimiter: '\t',
		})

		return Buffer.from(tsv, 'utf-8')
	}
}

