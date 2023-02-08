import { format } from 'mysql2'
import * as xml2js from 'xml2js'

import { iFeed } from '../feed'
import { MySQL } from '../../utils'
import { S3Client } from '../../utils'
import Constants from './constants'
import TSVFormat from './tsv-format'
import TSVData from './tsv-data'
import moment from 'moment'

const constants = new Constants()

export class CoochaFeed implements iFeed {
	static brandSemiNameMap: any
	static categorySemiNameMap: any

	async upload() {
		const buffer = await this.getTsvBuffer()

		const feedUrl = await S3Client.upload({
			folderName: 'feeds',
			fileName: 'coocha-feed.xml',
			buffer,
		})

		console.log(`FEED_URL: ${feedUrl}`)
	}

	async getTsvBuffer(delimiter = '\t'): Promise<Buffer> {
		return Buffer.from(await this.getTsv(delimiter), 'utf-8')
	}

	async getTsv(delimiter = '\t'): Promise<string> {
		const data = await MySQL.execute(CoochaFeed.query())
		const brandSemiNameRaw = await MySQL.execute('SELECT brand_id AS brandId, JSON_ARRAYAGG(semi_name) AS semiName FROM brand_search_name GROUP BY brand_id')
		const categorySemiNameRaw = await MySQL.execute('SELECT category AS categoryId, JSON_ARRAYAGG(semi_name) AS semiName FROM category_semi_name GROUP BY category')

		CoochaFeed.brandSemiNameMap = Object.fromEntries(brandSemiNameRaw.map(row => [row.brandId, row.semiName]))
		CoochaFeed.categorySemiNameMap = Object.fromEntries(categorySemiNameRaw.map(row => [row.categoryId, row.semiName]))
		const tsvData: TSVData[] = await Promise.all(
			data.filter(row => row.option_detail).map(CoochaFeed.makeRow),
		)

		const builder = new xml2js.Builder({
			cdata: true
		})

		return builder.buildObject({xml: {products: {product: tsvData}}})
	}

	private static query(): string {
		return format(
			`
			SELECT ii.idx AS 'id',
			       ii.shop_id AS shop_id,
						 ii.item_code AS item_code,
				
			       bi.brand_id,
			       bi.main_name,
						 bi.brand_name,
						 bi.brand_name_kor,
			       ii.item_gender,
			       fc.fetching_category_name,
			       ii.item_name,
			       ii.origin_name,
			       ii.custom_color,
			       ii.item_description AS description,
			       idsi.raw_id AS designer_style_id,
				
			       ip.final_price AS 'ip_final_price',
			       iop.final_price AS 'iop_final_price',

						 iop.total_price AS 'iop_total_price',

			       ii.image_url AS 'image_link',
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
                 SELECT fc.coocha_cname
                 FROM fetching_category fc
                          JOIN item_category_map icm on fc.idx = icm.fetching_category_id
                 WHERE icm.item_id = ii.idx
                   AND fc.fetching_category_depth = 2
                 LIMIT 1
             ) AS 'category_cname',
			       (
			           SELECT SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(i.size_name, '^', CEIL((ip.final_price + IFNULL(i.optional_price, 0)) * 0.97 / 100) * 100) SEPARATOR '|'), ',', 10)
			           FROM item_size i
			           WHERE i.item_id = ii.idx
			             AND i.price_rule = isp.price_rule
			          	 AND i.size_quantity > 0
			       ) AS 'option_detail'
			FROM coocha_upload_list nul USE INDEX (coocha_upload_list_sequence_index)
			    JOIN item_info ii on nul.item_id = ii.idx
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
			    LEFT JOIN item_designer_style_id idsi ON ii.idx = idsi.item_id
			WHERE ii.is_sellable = 1 AND ii.is_show = 1
				AND NOT (bi.brand_id = 17 AND (fc.idx IN (17, 21) OR fc.fetching_category_parent_id IN (17, 21)))
			ORDER BY nul.sequence
			LIMIT ?
		`,
			[constants.limit()],
		)
	}

	private static async makeRow(row): Promise<TSVData> {
		// eslint-disable-next-line camelcase
		if (row.designer_style_id) row.designer_style_id = row.designer_style_id.replace(/[^\dA-Za-z]/g, ' ').split(' ').filter(str => str).join(' ')

		const tsvFormat = new TSVFormat({
			itemGender: row.item_gender,
			id: row.id,
			productNo: row.product_no,
			shopId: row.shop_id,
		})
		const title: string = await tsvFormat.title({
			idx: row.id,
			shopId: row.shop_id,
			itemCode: row.item_code,
			mainName: row.main_name,
			brandName: row.brand_name,
			brandNameKor: row.brand_name_kor,
			lastCategory:
				row.category_name3 === '기타'
					? row.category_name2
					: row.category_name3,
			itemName: row.item_name,
			customColor: row.custom_color,
			mpn: row.designer_style_id,
		})
		const pcLink: string = tsvFormat.link({
			address: constants.address(),
		})
		const mobileLink: string = pcLink

		let price = tsvFormat.price(row.ip_final_price)
		let priceMobile = tsvFormat.priceMobile(row.ip_final_price)

		// 이미지 리사이징 버전으로 교체
		row['image_link'] = row.image_link.replace(
			'fetching-app.s3.ap-northeast-2.amazonaws.com',
			'static.fetchingapp.co.kr/resize/naver',
		)

		const [category1, category2, category3, category4] = row.category_cname?.split(',') ?? []

		return {
			'product_id': CoochaFeed.makeCdata(row.id),
			'product_title': CoochaFeed.makeCdata(title),
			'product_desc': CoochaFeed.makeCdata(row.description),
			'product_url': CoochaFeed.makeCdata(pcLink),
			'mobile_url': CoochaFeed.makeCdata(mobileLink),
			'sale_start': CoochaFeed.makeCdata(moment().format('yyyy-MM-DD HH:mm:ss')),
			'sale_end': CoochaFeed.makeCdata(moment().add(2, 'days').format('yyyy-MM-DD HH:mm:ss')),
			'price_normal': CoochaFeed.makeCdata(row.iop_final_price),
			'price_discount': CoochaFeed.makeCdata(price),
			'discount_rate': CoochaFeed.makeCdata(Math.round((1 - price / row.iop_final_price) * 100)),
			'coupon_use_start': '',
			'coupon_use_end': '',
			categorys: {
				category: {
					category1: category1 ?? '',
					category2: category2 ?? '',
					category3: category3 ?? '',
					category4: category4 ?? '',
				},
			},
			'buy_limit': '0',
			'buy_max': '999999',
			'buy_count': '0',
			'free_shipping': constants.shipping(),
			'image_url1': CoochaFeed.makeCdata(row.image_link),
			shops: {
				shop: {
					'shop_name': '',
					'shop_tel': '',
					'shop_address': '',
					'shop_latitude': '',
					'shop_longitude': '',
				}
			},
			'm_dcratio': CoochaFeed.makeCdata(Math.round((1 - priceMobile / row.iop_final_price) * 100)),
			'm_dcprice': CoochaFeed.makeCdata(priceMobile)
		}
	}

	private static makeCdata(string: any): string {
		string = `${string}`.replace(/[\0-\x08\x0B\f\x0E-\x1F\uFFFE\uFFFF]|[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?:[^\uD800-\uDBFF]|^)[\uDC00-\uDFFF]/, '')
		return string
	}
}
