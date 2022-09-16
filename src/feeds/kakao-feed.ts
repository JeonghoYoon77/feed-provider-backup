import { iFeed } from './feed'
import { MySQL, MySQLWrite, S3Client } from '../utils'
import moment from 'moment'

export class KakaoFeed implements iFeed {
	async upload() {
		const buffer = await this.getTsvBuffer()

		const feedUrl = await S3Client.upload({
			folderName: 'feeds',
			fileName: 'kakao-feed.txt',
			buffer,
			contentType: 'text/plain',
		})

		await S3Client.upload({
			folderName: 'feeds',
			fileName: 'kakao-update-feed.txt',
			buffer: Buffer.from(''),
			contentType: 'text/plain',
		})
		console.log(`FEED_URL: ${feedUrl}`)
	}

	async getTsvBuffer(): Promise<Buffer> {
		return Buffer.from(await this.getTsv(), 'utf-8')
	}

	async getTsv() {
		const limit = 500000
		const query = `
        SELECT ii.idx                                                                              AS 'id',
               ii.shop_id                                                                          AS 'shop_id',
               ii.item_code                                                                        AS 'code',
               bi.main_name                                                                        AS main_name,
               bi.brand_name                                                                       AS brand_name,
               bi.brand_name_kor                                                                   AS brand_name_kor,
               IF(ii.item_gender = 'W', '여성', '남성')                                                AS gender,
               idsi.designer_style_id                                                              AS mpn,
               ii.item_name                                                                        AS 'title',
               ii.custom_color                                                                     AS color,
               ip.final_price                                                                      AS original_price,
               CEIL(ip.final_price * 0.95 / 100) * 100                                             AS 'price_pc',
               CEIL(ip.final_price * 0.94 / 100) * 100                                             AS 'price_mobile',
               iop.final_price                                                                     AS 'normal_price',
               CONCAT('https://fetching.co.kr/product/', ii.idx)                                   as 'link',
               ii.image_url                                                                        AS 'image_link',
               dm.period                                                                           AS 'delivery_term',
               (
                   SELECT fc.fetching_category_name
                   FROM fetching_category fc
                            JOIN item_category_map icm on fc.idx = icm.fetching_category_id
                   WHERE icm.item_id = ii.idx
                     AND fc.fetching_category_depth = 0
                   LIMIT 1
               )                                                                                   AS 'category_name1',
               (
                   SELECT fc.idx
                   FROM fetching_category fc
                            JOIN item_category_map icm on fc.idx = icm.fetching_category_id
                   WHERE icm.item_id = ii.idx
                     AND fc.fetching_category_depth = 0
                   LIMIT 1
               )                                                                                   AS 'category_id1',
               (
                   SELECT fc.fetching_category_name
                   FROM fetching_category fc
                            JOIN item_category_map icm on fc.idx = icm.fetching_category_id
                   WHERE icm.item_id = ii.idx
                     AND fc.fetching_category_depth = 1
                   LIMIT 1
               )                                                                                   AS 'category_name2',
               (
                   SELECT fc.idx
                   FROM fetching_category fc
                            JOIN item_category_map icm on fc.idx = icm.fetching_category_id
                   WHERE icm.item_id = ii.idx
                     AND fc.fetching_category_depth = 1
                   LIMIT 1
               )                                                                                   AS 'category_id2',
               (
                   SELECT fc.fetching_category_name
                   FROM fetching_category fc
                            JOIN item_category_map icm on fc.idx = icm.fetching_category_id
                   WHERE icm.item_id = ii.idx
                     AND fc.fetching_category_depth = 2
                   LIMIT 1
               )                                                                                   AS 'category_name3',
               (
                   SELECT fc.idx
                   FROM fetching_category fc
                            JOIN item_category_map icm on fc.idx = icm.fetching_category_id
                   WHERE icm.item_id = ii.idx
                     AND fc.fetching_category_depth = 2
                   LIMIT 1
               )                                                                                   AS 'category_id3',
               (SELECT JSON_ARRAYAGG(cr.rating) FROM commerce.review cr WHERE ii.idx = cr.item_id) AS reviews,
               (SELECT COUNT(*) FROM commerce.item_order io WHERE ii.idx = io.item_id)             AS sales,
               (SELECT COUNT(*) FROM commerce.user_interest_item uii WHERE ii.idx = uii.item_id)   AS likeCount,
               ii.created_at                                                                       AS createdAt
        FROM item_info ii
                 LEFT JOIN item_designer_style_id idsi on idsi.item_id = ii.idx
                 JOIN brand_info bi on ii.brand_id = bi.brand_id
                 JOIN item_show_price isp on ii.idx = isp.item_id
                 JOIN shop_price sp on isp.price_rule = sp.idx
                 JOIn delivery_method dm on sp.delivery_method = dm.idx
                 JOIN item_price ip on ii.idx = ip.item_id AND isp.price_rule = ip.price_rule
                 JOIN item_user_price iup on ii.idx = iup.item_id AND ii.shop_id = iup.price_rule
                 JOIN item_origin_price iop on ii.idx = iop.item_id AND ii.shop_id = iop.price_rule
                 JOIN kakao_upload_list nul on ii.idx = nul.item_id
        WHERE ii.is_sellable = 1
        ORDER BY nul.sequence
        LIMIT ${limit}
		`
		const data = await MySQL.execute(query)

		const insertData = data.map((row) => [row.id, row.original_price])

		await MySQLWrite.execute('DELETE FROM kakao_upload_item_actual')
		await MySQLWrite.execute(`
			INSERT INTO kakao_upload_item_actual (item_id, final_price)
			VALUES ?;
		`, [insertData])

		let txt = [`<<<tocnt>>>${data.length}`]

		data.forEach((row) => {
			// 이미지 리사이징 버전으로 교체
			row['image_link'] = row.image_link.replace(
				'fetching-app.s3.ap-northeast-2.amazonaws.com',
				'static.fetchingapp.co.kr/resize/naver',
			)

			const category = !row.category_name3 || row.category_name3 === '기타'
				? row.category_name2
				: row.category_name3

			if (row.title.search(/[ㄱ-ㅎㅏ-ㅣ가-힣]/) === -1)
				row.title = ''
			else row.title = `${category} ${row.title}`
			row.title = row.title.trim()

			if (row.title.includes(row.brand_name))
				row.title = row.title.replace(row.brand_name, '').trim()
			if (row.title.includes(row.brand_name_kor))
				row.title = row.title.replace(row.brand_name_kor, '').trim()

			let title = `${row.main_name} ${row.title} ${
				row.mpn
					? row.mpn
					: [72, 78, 80].includes(row.shop_id)
						? ''
						: row.code
			} ${(row.color || '')
				.replace(/[^a-zA-Zㄱ-ㅎㅏ-ㅣ가-힣]/gi, ' ')
				.toUpperCase()
				.trim()}`
				.split(' ')
				.filter((str) => str)
				.join(' ')

			title = title.replace('è', 'e')
			title = title.replace('É', 'E')
			title = title.split('\n').join('')

			row.title = title
				.replace(/([&"'_])/g, '')
				.split(' ')
				.filter((data) => data)
				.join(' ')

			row['delivery_term'] = row.delivery_term.replace(/[^\d~]/g, '').split('~').pop()

			const rating = row.ratings.length ? row.ratings.reduce((a, b) => a + b, 0) / row.ratings.length : 5
			const reviewCount = row.reviews.length || 3 + Math.round(7 * Math.random())

			const sales = row.sales || 3 + Math.round(17 * Math.random())
			const likeCount = row.likeCount || 10 + Math.round(20 * Math.random())

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
<<<pgurl>>>${row.link.href}
<<<igurl>>>${row.image_link}
<<<cate1>>>${row.category_name1}
<<<caid1>>>${row.category_id1}
<<<cate2>>>${row.category_name2}
<<<caid2>>>${row.category_id2}
<<<cate3>>>${row.category_name3}
<<<caid3>>>${row.category_id3}
<<<model>>>${row.mpn ?? ''}
<<<brand>>>${row.main_name.replace(/ /g, '')}
<<<maker>>>${row.brand_name}
<<<coupo>>>5%
<<<mcoupon>>>6%
<<<pcard>>>삼성2~6,BC2~7,KB2~7,신한2~7,현대2~7,하나2~8,롯데2~4,NH2~8
<<<point>>>>2%
<<<deliv>>>0
<<<delivterm>>>${row.delivery_term}
<<<rating>>>${rating}
<<<revct>>>${reviewCount}
<<<event>>>#20만원 즉시 할인 #전상품 무료배송 #2% 적립 #유럽 공홈 #최대 80% 할인 #카드사별 2~8개월 무이자 혜택
<<<sales>>>${sales}
<<<likecnt>>>${likeCount}
<<<pubdate>>>${moment(row.createdAt).format('YYYYMMDD')}
<<<ftend>>>`)
		})

		return txt.join('\n')
	}
}
