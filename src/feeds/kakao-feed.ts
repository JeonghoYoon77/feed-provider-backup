import { iFeed } from './feed'
import {MySQL, MySQLWrite, S3Client} from '../utils'


export class KakaoFeed implements iFeed {
	async upload() {
		const buffer = await this.getTsvBuffer()

		const feedUrl = await S3Client.upload({
			folderName: 'feeds',
			fileName: 'kakao-feed.txt',
			buffer,
			contentType: 'text/plain'
		})

		await S3Client.upload({
			folderName: 'feeds',
			fileName: 'kakao-update-feed.txt',
			buffer: Buffer.from(''),
			contentType: 'text/plain'
		})
		console.log(`FEED_URL: ${feedUrl}`)
	}

	async getTsvBuffer(): Promise<Buffer> {
		return Buffer.from(await this.getTsv(), 'utf-8')
	}

	async getTsv() {
		const limit = 1000000
		const query = `
			SELECT ii.idx                                                                               AS 'id',
						 ii.shop_id                                                                           AS 'shop_id',
						 ii.item_code                                                                         AS 'code',
						 bi.main_name                                                                         AS brand_name,
						 IF(ii.item_gender = 'W', '여성', '남성')                                                 AS gender,
						 idsi.designer_style_id                                                               AS mpn,
						 ii.item_name                                                                         AS 'title',
						 ii.custom_color                                                                      AS color,
						 IF(cud.product_no, CEIL(cui.final_price * 0.97 / 100) * 100, iup.total_price)        AS 'price_pc',
						 IF(cud.product_no, CEIL(cui.final_price * 0.97 / 100) * 100, iup.total_price)        AS 'price_mobile',
						 IF(cud.product_no, CEIL(cui.origin_final_price * 0.97 / 100) * 100, iop.total_price) AS 'normal_price',
						 IF(cud.product_no,
								CONCAT('https://m.fetching.co.kr/product/detail.html?product_no=', cud.product_no),
								CONCAT('https://m.fetching.co.kr/product_detail_app.html?product_no=', ii.idx)
							 )                                                                                  as 'link',
						 ii.image_url                                                                         AS 'image_link',
						 (
							 SELECT fc.fetching_category_name
							 FROM fetching_category fc
											JOIN item_category_map icm on fc.idx = icm.fetching_category_id
							 WHERE icm.item_id = ii.idx
								 AND fc.fetching_category_depth = 0
							 LIMIT 1
						 )                                                                                    AS 'category_name1',
						 (
							 SELECT fc.idx
							 FROM fetching_category fc
											JOIN item_category_map icm on fc.idx = icm.fetching_category_id
							 WHERE icm.item_id = ii.idx
								 AND fc.fetching_category_depth = 0
							 LIMIT 1
						 )                                                                                    AS 'category_id1',
						 (
							 SELECT fc.fetching_category_name
							 FROM fetching_category fc
											JOIN item_category_map icm on fc.idx = icm.fetching_category_id
							 WHERE icm.item_id = ii.idx
								 AND fc.fetching_category_depth = 1
							 LIMIT 1
						 )                                                                                    AS 'category_name2',
						 (
							 SELECT fc.idx
							 FROM fetching_category fc
											JOIN item_category_map icm on fc.idx = icm.fetching_category_id
							 WHERE icm.item_id = ii.idx
								 AND fc.fetching_category_depth = 1
							 LIMIT 1
						 )                                                                                    AS 'category_id2',
						 (
							 SELECT fc.fetching_category_name
							 FROM fetching_category fc
											JOIN item_category_map icm on fc.idx = icm.fetching_category_id
							 WHERE icm.item_id = ii.idx
								 AND fc.fetching_category_depth = 2
							 LIMIT 1
						 )                                                                                    AS 'category_name3',
						 (
							 SELECT fc.idx
							 FROM fetching_category fc
											JOIN item_category_map icm on fc.idx = icm.fetching_category_id
							 WHERE icm.item_id = ii.idx
								 AND fc.fetching_category_depth = 2
							 LIMIT 1
						 )                                                                                    AS 'category_id3',
						 '100% 정품, 관부가세 포함, 기한한정 세일!'                                                    AS 'event_words'
			FROM item_info ii
						 LEFT JOIN cafe24_upload_db cud on cud.item_id = ii.idx AND cud.is_active = 1
						 LEFT JOIN cafe24_upload_info cui on cui.item_id = cud.item_id
						 LEFT JOIN item_designer_style_id idsi on idsi.item_id = ii.idx
						 JOIN brand_info bi on ii.brand_id = bi.brand_id
						 JOIN item_show_price isp on ii.idx = isp.item_id
						 JOIN item_price ip on ii.idx = ip.item_id AND isp.price_rule = ip.price_rule
						 JOIN item_user_price iup on ii.idx = iup.item_id
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
						 LEFT JOIN naver_upload_list nul on ii.idx = nul.item_id
			WHERE ii.is_verify = 1
			ORDER BY nul.sequence
			LIMIT ${limit}
		`
		const data = await MySQL.execute(query)

		const insertData = data.map(row => [row.id])

		await MySQLWrite.execute('DELETE FROM kakao_upload_item')
		await MySQLWrite.execute(`
			INSERT INTO kakao_upload_item (item_id, final_price)
			SELECT idx, final_price
			FROM item_info ii
						 JOIN item_show_price isp on ii.idx = isp.item_id
						 JOIN item_price ip ON ii.idx = ip.item_id AND isp.price_rule = ip.price_rule
			WHERE ii.idx IN (?);	
		`, [insertData])

		let txt = [`<<<tocnt>>>${data.length}`]

		data.forEach((row) => {
			if (row.title.search(/[ㄱ-ㅎㅏ-ㅣ가-힣]/) === -1) row.title = (!row.category_name3 || row.category_name3 === '기타') ? row.category_name2 : row.category_name3
			row.title = row.title.trim()

			let title = `${row.brand_name} ${row.gender} ${row.title} ${row.mpn ? row.mpn : [72, 78, 80].includes(row.shop_id) ? '' : row.code} ${(row.color || '').replace(/[^a-zA-Zㄱ-ㅎㅏ-ㅣ가-힣]/gi, ' ').toUpperCase().trim()}`
				.split(' ').filter(str => str).join(' ')

			title = title.replace('è', 'e')
			title = title.replace('É', 'E')
			title = title.split('\n').join('')

			row.title = title.replace(/([&"'_])/g, '').split(' ').filter(data => data).join(' ')

			row.link = new URL(row.link)
			row.link.searchParams.set('utm_source', 'daum')
			row.link.searchParams.set('utm_medium', 'cpc')
			row.link.searchParams.set('utm_campaign', 'shoppinghow')
			txt.push(`<<<begin>>>
<<<mapid>>>${row.id}
<<<lprice>>>${row.normal_price}
<<<price>>>${row.price_pc}
<<<mpric>>>${row.price_mobile}
<<<pname>>>${row.title}
<<<pgurl>>>${row.link.toString()}
<<<igurl>>>${row.image_link}
<<<cate1>>>${row.category_name1}
<<<caid1>>>${row.category_id1}
<<<cate2>>>${row.category_name2}
<<<caid2>>>${row.category_id2}
<<<cate3>>>${row.category_name3}
<<<caid3>>>${row.category_id3}
<<<brand>>>${row.brand_name}
<<<deliv>>>0
<<<event>>>${row.event_words}
<<<ftend>>>`)
		}
		)

		return txt.join('\n')
	}
}

