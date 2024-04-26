import { parse } from 'json2csv'
import { format } from 'mysql2'

import { iFeed } from '../feed'
import {MySQL, MySQLWrite} from '../../utils'
import { S3Client } from '../../utils'
import Constants from './constants'
import TSVFormat from './tsv-format'
import TSVData from './tsv-data'
import * as fs from 'fs'
import {chunk} from 'lodash'

const constants = new Constants()

export class NaverFeed implements iFeed {
	static brandSemiNameMap: any
	static categorySemiNameMap: any

	private chunkedUpdate: any[] = []

	async upload() {
		const readStream = await this.getTsvBuffer()

		const feedUrl = await S3Client.upload({
			folderName: 'feeds',
			fileName: 'naver-feed.tsv',
			readStream,
		})

		console.log('FEED_URL\t:', feedUrl)
		await S3Client.upload({
			folderName: 'feeds',
			fileName: 'naver-update-feed.tsv',
			buffer: Buffer.from(''),
			contentType: 'text/plain',
		})


		await MySQLWrite.execute('DELETE FROM naver_upload_item_actual')
		for (const i in this.chunkedUpdate) {
			console.log('PROCESS\t:', parseInt(i) + 1, '/', this.chunkedUpdate.length)

			await MySQLWrite.execute(`
        INSERT IGNORE INTO naver_upload_item_actual (item_id, final_price)
        VALUES ?;
      `, [this.chunkedUpdate[i]])
		}
	}

	async getTsvBuffer(delimiter = '\t'): Promise<fs.ReadStream> {
		return this.getTsv(delimiter)
	}

	async getTsv(delimiter = '\t'): Promise<fs.ReadStream> {
		try {
			fs.unlinkSync('./naver-feed.tsv')
		} catch {}
		fs.writeFileSync('./naver-feed.tsv', '')
		const brandSemiNameRaw = await MySQL.execute('SELECT brand_id AS brandId, JSON_ARRAYAGG(semi_name) AS semiName FROM brand_search_name GROUP BY brand_id')
		const categorySemiNameRaw = await MySQL.execute('SELECT category AS categoryId, JSON_ARRAYAGG(semi_name) AS semiName FROM category_semi_name GROUP BY category')

		NaverFeed.brandSemiNameMap = Object.fromEntries(brandSemiNameRaw.map(row => [row.brandId, row.semiName]))
		NaverFeed.categorySemiNameMap = Object.fromEntries(categorySemiNameRaw.map(row => [row.categoryId, row.semiName]))

		const listRaw = await MySQL.execute('SELECT item_id FROM naver_upload_list nul')
		const list = listRaw.map(row => row.item_id)
		const chunkedList = chunk(list, 100000)

		let completed = 0
		const tsvDataList = await Promise.all(chunkedList.map(async list => {
			const data = await MySQL.execute(NaverFeed.query(list))
			const currentData = data.filter(row => row.option_detail && row.category_name1 && row.category_name2 && row.category_name3 && !(row.brand_id === 17 && row.category_name2 === '악세서리'))
			this.chunkedUpdate.push(currentData.map(row => [row.id, row.ip_final_price]))
			const tsvData: TSVData[] = (await Promise.all(currentData.map(NaverFeed.makeRow))).filter(row => row)
			console.log('PROCESS\t:', 'SELECT', ++completed, '/', chunkedList.length)
			return tsvData
		}))

		for (let i in tsvDataList) {
			const tsvData: TSVData[] = tsvDataList[i]
			console.log('PROCESS\t:', 'FILE', parseInt(i) + 1, '/', chunkedList.length)
			fs.appendFileSync('./naver-feed.tsv', parse(tsvData, {
				fields: Object.keys(tsvData[0]),
				header: i === '0',
				delimiter,
				quote: '',
			}))
		}
		return fs.createReadStream('./naver-feed.tsv')
	}

	private static query(itemIds): string {
		return format(`
			SELECT STRAIGHT_JOIN ii.idx                                                              AS 'id',
						 ii.shop_id                                                          AS shop_id,
						 ii.item_code                                                        AS item_code,

						 bi.brand_id,
						 bi.main_name,
						 bi.brand_name,
						 bi.brand_name_kor,
						 ii.item_gender,
						 ii.item_name,
						 ii.origin_name,
						 ii.custom_color,
						 idsi.raw_id                                                         AS designer_style_id,
						 idsi.raw_color_id                                                   AS designer_color_id,
						 season,
						 inpi.naver_product_id,

						 si.shop_type,

						 ci.country_name,

						 ip.final_price                                                      AS 'ip_final_price',
						 iop.final_price                                                     AS 'iop_final_price',

						 iup.total_price                                                     AS 'iup_total_price',
						 iop.total_price                                                     AS 'iop_total_price',

						 ii.image_url                                                        AS 'image_link',
						 (SELECT SUBSTRING_INDEX(GROUP_CONCAT(REPLACE(ig.item_image_url, ',', '%2C') SEPARATOR '|'),
																		 '|', 10)
							FROM item_image ig
							WHERE ig.item_id = ii.idx
							ORDER BY ig.priority ASC)                                          AS 'add_image_link',
						 (SELECT fc.fetching_category_name
							FROM fetching_category fc
										 JOIN item_category_map icm on fc.idx = icm.fetching_category_id
							WHERE icm.item_id = ii.idx
								AND fc.fetching_category_depth = 0
							LIMIT 1)                                                           AS 'category_name1',
						 (SELECT fc.fetching_category_name
							FROM fetching_category fc
										 JOIN item_category_map icm on fc.idx = icm.fetching_category_id
							WHERE icm.item_id = ii.idx
								AND fc.fetching_category_depth = 1
							LIMIT 1)                                                           AS 'category_name2',
						 (SELECT fc.fetching_category_name
							FROM fetching_category fc
										 JOIN item_category_map icm on fc.idx = icm.fetching_category_id
							WHERE icm.item_id = ii.idx
								AND fc.fetching_category_depth = 2
							LIMIT 1)                                                           AS 'category_name3',
						 (SELECT fc.idx
							FROM fetching_category fc
										 JOIN item_category_map icm on fc.idx = icm.fetching_category_id
							WHERE icm.item_id = ii.idx
								AND fc.fetching_category_depth = 2
							LIMIT 1)                                                           AS 'category_id3',
						 (SELECT fc.smartstore_category_id
							FROM fetching_category fc
										 JOIN item_category_map icm on fc.idx = icm.fetching_category_id
							WHERE icm.item_id = ii.idx
							ORDER BY icm.fetching_category_id DESC
							LIMIT 1)                                                           AS 'naver_category',
						 (SELECT SUBSTRING_INDEX(GROUP_CONCAT(CONCAT(i.size_name, '^',
																												 CEIL((ip.final_price + IFNULL(i.optional_price, 0)) * 0.97 / 100) *
																												 100) SEPARATOR '|'), ',', 10)
							FROM item_size i
							WHERE i.item_id = ii.idx
								AND i.price_rule = isp.price_rule
								AND i.size_quantity > 0)                                         AS 'option_detail',
						 (SELECT COUNT(*) FROM commerce.review cr WHERE ii.idx = cr.item_id) AS review_count,
						 IF(iif.item_id IS NULL, 'Y', 'N')                                   AS import_flag
			FROM item_info ii
						 JOIN item_show_price isp on ii.idx = isp.item_id
						 JOIN shop_info si on ii.shop_id = si.shop_id
						 JOIN country_info ci on ii.item_country = ci.country_tag
						 JOIN brand_info bi on ii.brand_id = bi.brand_id
						 JOIN item_price ip on ii.idx = ip.item_id AND isp.price_rule = ip.price_rule
						 JOIN item_user_price iup on ii.idx = iup.item_id AND isp.price_rule = iup.price_rule
						 JOIN item_origin_price iop on ii.idx = iop.item_id AND isp.price_rule = iop.price_rule
						 LEFT JOIN item_import_flag iif ON iif.item_id = ii.idx
						 LEFT JOIN item_designer_style_id idsi ON ii.idx = idsi.item_id
			       LEFT JOIN item_seasons season ON ii.idx = season.item_id
						 LEFT JOIN item_naver_product_id inpi on ii.idx = inpi.idx
			WHERE ii.is_sellable = 1
				AND ii.is_show = 1
				AND ii.idx IN (?)
		`, [itemIds])
	}

	private static async makeRow(row): Promise<TSVData> {
		if (!row.option_detail) return
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
			mpn: [row.designer_style_id, row.designer_color_id].filter(str => str).join(' '),
			season: row.season,
		})
		const pcLink: string = tsvFormat.link({
			address: constants.address(),
		})
		const mobileLink: string = tsvFormat.mobileLink({
			address: constants.address(),
		})
		const searchTag: string = tsvFormat.searchTag({
			brandName: row.brand_name, brandNameKor: row.brand_name_kor, categoryName2: row.category_name2, categoryName3: row.category_name3, color: tsvFormat.color(row.custom_color), designerStyleId: row.designer_style_id, originName: row.origin_name, itemName: row.item_name, brandSemiName: NaverFeed.brandSemiNameMap[row.brand_id], categorySemiName: NaverFeed.categorySemiNameMap[row.category_id3]
		})

		let price = tsvFormat.price(row.ip_final_price)
		let priceMobile = tsvFormat.priceMobile(row.ip_final_price)
		let point = Math.floor(price * 0.01)

		// 이미지 리사이징 버전으로 교체
		row['image_link'] = row.image_link.replace(
			'fetching-app.s3.ap-northeast-2.amazonaws.com',
			'static.fetchingapp.co.kr/resize/naver',
		)

		return {
			id: `F${row.id}`,
			title,
			'price_pc': price,
			'price_mobile': priceMobile,
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
			brand: row.main_name,
			'event_words': constants.eventWords(),
			coupon: tsvFormat.coupon(row.ip_final_price),
			'partner_coupon_download': row.product_no
				? tsvFormat.partnerCouponDownload(row.ip_final_price)
				: '',
			'interest_free_event':
				'현대카드^2~3|BC카드^2~4|KB국민카드^2~3|하나카드^2~3|NH농협카드^2~4|신한카드^2~3|롯데카드^2~3|삼성카드^2~3|우리카드^2~4',
			point,
			'manufacture_define_number': row.designer_style_id || '',
			'naver_product_id': row.naver_product_id || '',
			origin: row.country_name === 'Unknown' ? '' : row.country_name,
			'review_count': row.review_count,
			shipping: constants.shipping(),
			'import_flag': ['해외편집샵', '해외부티크', '해외브랜드'].includes(row.shop_type) ? row.import_flag : 'N',
			'option_detail': row.option_detail
				?.split('\n')
				?.filter((str) => str)
				?.join(' ')
				?.split('\t')
				?.filter((str) => str)
				?.join('') ?? '',
			gender: tsvFormat.gender(),
			'includes_vat': constants.includesVat(),
			'search_tag': searchTag,
			maker: row.brand_name,
		}
	}
}

