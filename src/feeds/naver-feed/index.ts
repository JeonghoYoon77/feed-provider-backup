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

	async getTsvBuffer(delimiter = '\t'): Promise<Buffer> {
		return Buffer.from(await this.getTsv(delimiter), 'utf-8')
	}

	async getTsv(delimiter = '\t'): Promise<string> {
		const data = await MySQL.execute(NaverFeed.query())
		const tsvData: TSVData[] = await Promise.all(data.map(NaverFeed.makeRow))

		return parse(tsvData, {
			fields: Object.keys(tsvData[0]),
			delimiter,
			quote: '',
		})
	}

	private static query(): string {
		return format(`
			SELECT ii.idx AS 'id',
			       ii.shop_id AS shop_id,
						 ii.item_code AS item_code,
				
			       bi.main_name,
						 bi.brand_name,
						 bi.brand_name_kor,
			       ii.item_gender,
			       fc.fetching_category_name,
			       ii.item_name,
			       ii.origin_name,
			       ii.custom_color,
			       idsi.designer_style_id,
						 inpi.naver_product_id,
			       
			       si.shop_type,
			       
			       ci.country_name,
				
			       ip.final_price AS 'ip_final_price',
			       iop.final_price AS 'iop_final_price',

						 iup.total_price AS 'iup_total_price',
						 iop.total_price AS 'iop_total_price',

			       ii.image_url AS 'image_link',
			       (
			           SELECT SUBSTRING_INDEX(GROUP_CONCAT(REPLACE(ig.item_image_url, ',', '%2C') SEPARATOR '|'), '|', 10)
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
			               LIMIT 1 OFFSET 1
			           ),
			           (
			               SELECT bsi.semi_name
			               FROM brand_semi_name bsi
			               WHERE bsi.brand_id = bi.brand_id
			               LIMIT 1 OFFSET 2
			      		 )
			       ), '\t', ' ') AS 'search_tag',
			       (SELECT COUNT(*) FROM commerce.review cr WHERE ii.idx = cr.item_id) AS review_count,
			       IF(iif.item_id IS NULL, 'Y', 'N') AS import_flag
			FROM naver_upload_list nul USE INDEX (naver_upload_list_sequence_index)
			    JOIN item_info ii on nul.item_id = ii.idx
			    LEFT JOIN cafe24_upload_db cud ON cud.item_id = nul.item_id
					JOIN item_show_price isp on ii.idx = isp.item_id
			    JOIN shop_info si on ii.shop_id = si.shop_id
			    JOIN country_info ci on ii.item_country = ci.country_tag
			    JOIN brand_info bi on ii.brand_id = bi.brand_id
			    JOIN item_price ip on ii.idx = ip.item_id AND isp.price_rule = ip.price_rule
					JOIN item_user_price iup on ii.idx = iup.item_id AND isp.price_rule = iup.price_rule
			    JOIN item_origin_price iop on ii.idx = iop.item_id AND isp.price_rule = iop.price_rule
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
			    LEFT JOIN item_designer_style_id idsi ON ii.idx = idsi.item_id
					LEFT JOIN item_naver_product_id inpi on ii.idx = inpi.idx
			WHERE ii.is_sellable = 1 AND ii.is_show = 1
				AND NOT (bi.brand_id = 17 AND (fc.idx IN (17, 21) OR fc.fetching_category_parent_id IN (17, 21)))
			ORDER BY nul.sequence
			LIMIT ?
		`, [constants.limit()])
	}

	private static async makeRow(row): Promise<TSVData> {
		const tsvFormat = new TSVFormat({
			itemGender: row.item_gender,
			id: row.id,
			productNo: row.product_no,
			shopId: row.shop_id
		})
		const title: string = await tsvFormat.title({
			shopId: row.shop_id,
			itemCode: row.item_code,
			mainName: row.main_name,
			brandName: row.brand_name,
			brandNameKor: row.brand_name_kor,
			lastCategory: row.category_name3 === '기타' ? row.category_name2 : row.category_name3,
			itemName: row.item_name,
			customColor: row.custom_color,
			mpn: row.designer_style_id,
		})
		const pcLink: string = tsvFormat.link({
			address: constants.address(),
		})
		const mobileLink: string = tsvFormat.link({
			address: constants.address(),
		})
		const searchTag: string = tsvFormat.searchTag({
			itemName: row.item_name,
			brandMainName: row.main_name,
			categoryName2: row.category_name2,
			categoryName3: row.category_name3,
		})

		let price = tsvFormat.price(row.ip_final_price)
		let point = Math.floor(price * 0.02)

		return {
			id: `F${row.id}`,
			title,
			'price_pc': price,
			'price_mobile': price,
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
			'brand': row.main_name,
			'event_words': constants.eventWords(),
			coupon: tsvFormat.coupon(row.ip_final_price),
			'partner_coupon_download': row.product_no ? tsvFormat.partnerCouponDownload(row.ip_final_price) : '',
			'interest_free_event': '삼성카드^2~6|BC카드^2~7|KB국민카드^2~7|신한카드^2~7|현대카드^2~7|하나카드^2~8|롯데카드^2~4|NH농협카드^2~6',
			point,
			'manufacture_define_number': row.designer_style_id || '',
			'naver_product_id': row.naver_product_id || '',
			origin: row.country_name === 'Unknown' ? '' : row.country_name,
			'review_count': row.review_count,
			shipping: constants.shipping(),
			'import_flag': row.shop_type === '해외편집샵' ? row.import_flag : 'N',
			'option_detail': row.option_detail.split('\n').filter(str => str).join(' '),
			gender: tsvFormat.gender(),
			'includes_vat': constants.includesVat(),
			'search_tag': searchTag
		}
	}
}

