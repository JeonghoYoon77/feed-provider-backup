import { parse } from 'json2csv'
import { format } from 'mysql2'

import { iFeed } from '../feed'
import { MySQL } from '../../utils'
import { S3Client } from '../../utils'
import Constants from './constants'
import TSVFormat from './tsv-format'
import TSVData from './tsv-data'
import * as fs from 'fs'

const constants = new Constants()

export class PiclickFeed implements iFeed {
	async upload() {
		const buffer = await this.getTsvBuffer()

		const feedUrl = await S3Client.upload({
			folderName: 'feeds',
			fileName: 'piclick-feed.tsv',
			buffer,
		})

		console.log(`FEED_URL: ${feedUrl}`)
	}

	async getTsvBuffer(delimiter = '\t'): Promise<Buffer> {
		return this.getTsv(delimiter)
	}

	async getTsv(delimiter = '\t'): Promise<Buffer> {
		try {
			fs.unlinkSync('./piclick-feed.csv')
		} catch {}
		let bookmark
		for (let skip = 0; true; skip += 100000) {
			const data = await MySQL.execute(PiclickFeed.query(100000, skip, bookmark))
			bookmark = data[data.length - 1].id
			const tsvData: TSVData[] = (await Promise.all(data.map(PiclickFeed.makeRow))).filter(row => row)
			console.log(data.length, bookmark)

			fs.appendFileSync('./piclick-feed.csv', parse(tsvData, {
				fields: Object.keys(tsvData[0]),
				header: skip === 0,
				delimiter,
				quote: '',
			}))
			if (data.length < 100000) break
		}
		return fs.readFileSync('./piclick-feed.csv')
	}

	private static query(limit, skip, bookmark): string {
		return format(`
			SELECT STRAIGHT_JOIN ii.idx AS 'id',
			       ii.shop_id AS shop_id,
						 ii.item_code AS item_code,
				
			       bi.main_name,
			       ii.item_gender,
			       fc.fetching_category_name,
			       ii.item_name,
			       ii.origin_name,
			       ii.custom_color,
			       idsi.raw_id AS designer_style_id,
						 idsi.raw_color_id AS designer_color_id,
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
			       IF(iif.item_id IS NULL, 'Y', 'N') AS import_flag
			FROM item_info ii
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
			WHERE ii.is_sellable = 1 AND ii.is_show = 1${bookmark ? ` AND ii.idx > ${bookmark}` : ''}
			ORDER BY ii.idx
			LIMIT ?
		`, [limit])
	}

	private static async makeRow(row): Promise<TSVData> {
		try {
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
				lastCategory: row.category_name3 === '기타' ? row.category_name2 : row.category_name3,
				itemName: row.item_name,
				customColor: row.custom_color,
				mpn: [row.designer_style_id, row.designer_color_id].filter(str => str).join(' '),
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
			let point = Math.floor(price * 0.01)

			return {
				id: `F${row.id}`,
				title,
				'price_pc': price,
				'price_mobile': price,
				'normal_price': price >= row.iop_final_price ? price : row.iop_final_price,
				link: pcLink,
				'mobile_link': mobileLink,
				'image_link': row.image_link.replace('fetching-app.s3.ap-northeast-2.amazonaws.com', 'static.fetchingapp.co.kr'),
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
				'interest_free_event': '삼성카드^2~3|현대카드^2~3|BC카드^2~3|KB국민카드^2~3|하나카드^2~3|NH농협카드^2~4|신한카드^2~3',
				point,
				'manufacture_define_number': row.designer_style_id || '',
				'naver_product_id': row.naver_product_id || '',
				origin: row.country_name === 'Unknown' ? '' : row.country_name,
				'review_count': row.review_count,
				shipping: constants.shipping(),
				'import_flag': ['해외편집샵', '해외부티크', '해외브랜드'].includes(row.shop_type) ? row.import_flag : 'N',
				'option_detail': row.option_detail.split('\n').filter(str => str).join(' '),
				gender: tsvFormat.gender(),
				'includes_vat': constants.includesVat(),
				'search_tag': ''
			}
		} catch {
			return null
		}
	}
}

