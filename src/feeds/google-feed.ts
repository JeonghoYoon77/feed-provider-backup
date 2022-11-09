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

		const naverFeedData = await MySQL.execute(`
        SELECT ii.idx
        FROM item_info ii
                 JOIN naver_upload_list nul on ii.idx = nul.item_id
        WHERE ii.is_sellable
          AND EXISTS(
                SELECT icm.fetching_category_id
                FROM item_category_map icm
                         JOIN fetching_category fc on icm.fetching_category_id = fc.idx
                WHERE icm.item_id = ii.idx
                  AND fc.google_category_id IS NOT NULL
                  AND fc.fetching_category_depth = 2
                ORDER BY icm.fetching_category_id DESC
            )
        ORDER BY nul.sequence
				LIMIT ?
		`, [limit])

		const naverFeedProducts = naverFeedData.map(row => row.idx)

		const othersData = await MySQL.execute(`
        SELECT ii.idx
        FROM item_info ii
                 JOIN item_recommend_score irs on ii.idx = irs.item_id
        WHERE ii.idx NOT IN (?)
        	AND ii.is_sellable
          AND EXISTS(
                SELECT icm.fetching_category_id
                FROM item_category_map icm
                         JOIN fetching_category fc on icm.fetching_category_id = fc.idx
                WHERE icm.item_id = ii.idx
                  AND fc.google_category_id IS NOT NULL
                  AND fc.fetching_category_depth = 2
                ORDER BY icm.fetching_category_id DESC
            )
        ORDER BY irs.score DESC
				LIMIT ?
		`, [naverFeedProducts, limit - naverFeedProducts.length])

		const otherProducts = othersData.map(row => row.idx)

		const query = `
        SELECT ii.idx                                                as 'id',
               REPLACE(ii.item_name, '\t', ' ')                      as 'title',
               REPLACE(ii.item_description, '\t', ' ')               as 'description',
               CONCAT('https://fetching.co.kr/product/', ii.idx)     as 'link',
               CONCAT('https://fetching.co.kr/product/', ii.idx)     as 'mobile_link',
               ii.image_url                                          as 'image_link',
               (SELECT SUBSTRING_INDEX(GROUP_CONCAT(REPLACE(ig.item_image_url, ',', '%2C') SEPARATOR ','), ',', 10)
                FROM item_image ig
                WHERE ig.item_id = ii.idx
                ORDER BY ig.priority ASC)                            as additional_image_link,
               (SELECT SUBSTRING_INDEX(GROUP_CONCAT(REPLACE(i.size_name, ',', '%2C') SEPARATOR ','), ',', 10)
                FROM item_size i
                WHERE i.item_id = ii.idx
                  AND i.size_quantity > 0)                           as size,
               IF(EXISTS((SELECT i.item_id
                          FROM item_size i
                          WHERE i.item_id = ii.idx
                            AND i.price_rule = isp.price_rule
                            AND i.size_quantity > 0
                          LIMIT 1)) > 0, 'in stock', 'out of stock') as 'availability',
               CONCAT(iop.final_price, '.00 KRW')                    as 'price',
               CONCAT(ip.final_price, '.00 KRW')                     as 'sale_price',
               (SELECT fc.google_category_id
                FROM item_category_map icm
                         JOIN fetching_category fc ON icm.fetching_category_id = fc.idx
                WHERE fc.google_category_id IS NOT NULL
                  AND fetching_category_depth = 2
                ORDER BY fc.idx
                LIMIT 1)                                             as 'google_product_category',
               (SELECT CONCAT_WS(' ', fc1.fetching_category_name, '>', fc2.fetching_category_name, '>', fc3.fetching_category_name)
                FROM item_category_map icm
                         JOIN fetching_category fc3 ON icm.fetching_category_id = fc3.idx
                         JOIN fetching_category fc2 ON fc3.idx = fc2.fetching_category_parent_id
                         JOIN fetching_category fc1 ON fc2.idx = fc1.fetching_category_parent_id
                WHERE fc3.google_category_id IS NOT NULL
                  AND fc3.fetching_category_depth = 2
                ORDER BY fc3.idx
                LIMIT 1)                                            as product_type,
               bi.brand_name_kor                                     as 'brand',
               idsi.raw_id                                           as 'MPN',
               'no'                                                  as 'adult',
               IF(ii.item_gender = 'W', 'female', 'male')            as 'gender',
               ii.idx                                                as 'item_group_id'
        FROM item_info ii
                 JOIN item_show_price isp on ii.idx = isp.item_id
                 JOIN item_price ip IGNORE INDEX (item_price_final_price_index)
                      on ii.idx = ip.item_id AND isp.price_rule = ip.price_rule
                 JOIN item_user_price iup on ii.idx = iup.item_id AND isp.price_rule = iup.price_rule
                 JOIN item_origin_price iop on ii.idx = iop.item_id AND isp.price_rule = iop.price_rule
                 JOIN brand_info bi on ii.brand_id = bi.brand_id
                 LEFT JOIN item_designer_style_id idsi on ii.idx = idsi.item_id
        WHERE ii.idx IN (?)
		`
		let data = await MySQL.execute(query, [[...naverFeedProducts, ...otherProducts]])

		data.map(row => {
			const link = new URL(row.link)
			link.searchParams.set('utm_source', 'google')
			link.searchParams.set('utm_medium', 'display')
			link.searchParams.set('utm_campaign', 'gdn')
			row.link = link.toString()

			const mobileLink = new URL(row.mobile_link)
			mobileLink.searchParams.set('utm_source', 'google')
			mobileLink.searchParams.set('utm_medium', 'display')
			mobileLink.searchParams.set('utm_campaign', 'gdn')
			row['mobile_link'] = mobileLink.toString()

			row['image_link'] = row.image_link.replace('fetching-app.s3.ap-northeast-2.amazonaws.com', 'static.fetchingapp.co.kr')
		})

		const tsv = parse(data, {
			fields: Object.keys(data[0]),
			delimiter: '\t',
		})

		return Buffer.from(tsv, 'utf-8')
	}
}

