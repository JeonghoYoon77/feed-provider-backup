import { iFeed } from './feed'
import { MySQL } from '../utils'
import { S3Client } from '../utils'

export class KakaoFeed implements iFeed {
	async upload() {
		const buffer = await this.getTsvBuffer()

		const feedUrl = await S3Client.upload({
			folderName: 'feeds',
			fileName: 'kakao-feed.txt',
			buffer,
		})
		console.log(`FEED_URL: ${feedUrl}`)
	}

	async getTsvBuffer(): Promise<Buffer> {
		return Buffer.from(await this.getTsv(), 'utf-8')
	}

	async getTsv() {
		const limit = 99999
		const query = `
			SELECT
				ii.idx AS 'id',
				REPLACE(REPLACE(CONCAT_WS(' ', bi.brand_name_kor, IF(ii.item_gender = 'W', '여성', '남성'), fc.fetching_category_name, ii.item_name), '
', ''), '\t', '') AS 'title',
				CEIL(ip.final_price * 0.97 / 100) * 100 AS 'price_pc',
				CEIL(ip.final_price * 0.97 / 100) * 100 AS 'price_mobile',
				CEIL(iop.final_price * 0.97 / 100) * 100 AS 'normal_price',
				CONCAT('https://fetching.co.kr/product/detail.html?product_no=', cud.product_no) AS 'link',
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
					SELECT fc.idx
					FROM fetching_category fc
					JOIN item_category_map icm on fc.idx = icm.fetching_category_id
					WHERE icm.item_id = ii.idx
						AND fc.fetching_category_depth = 0
					LIMIT 1
				) AS 'category_id1',
				(
					SELECT fc.fetching_category_name
					FROM fetching_category fc
					JOIN item_category_map icm on fc.idx = icm.fetching_category_id
					WHERE icm.item_id = ii.idx
						AND fc.fetching_category_depth = 1
					LIMIT 1
				) AS 'category_name2',
				(
					SELECT fc.idx
					FROM fetching_category fc
					JOIN item_category_map icm on fc.idx = icm.fetching_category_id
					WHERE icm.item_id = ii.idx
						AND fc.fetching_category_depth = 1
					LIMIT 1
				) AS 'category_id2',
				(
					SELECT fc.fetching_category_name
					FROM fetching_category fc
					JOIN item_category_map icm on fc.idx = icm.fetching_category_id
					WHERE icm.item_id = ii.idx
						AND fc.fetching_category_depth = 2
					LIMIT 1
				) AS 'category_name3',
				(
					SELECT fc.idx
					FROM fetching_category fc
					JOIN item_category_map icm on fc.idx = icm.fetching_category_id
					WHERE icm.item_id = ii.idx
						AND fc.fetching_category_depth = 2
					LIMIT 1
				) AS 'category_id3',
				bi.brand_name_kor AS 'brand_name',
				'100% 정품, 관부가세 포함, 기한한정 세일!' AS 'event_words'
			FROM cafe24_upload_db cud
			JOIN item_info ii on cud.item_id = ii.idx
			JOIN brand_info bi on ii.brand_id = bi.brand_id
			JOIN item_price ip on ii.idx = ip.item_id
			JOIN item_origin_price iop on ii.idx = iop.item_id
			JOIN cafe24_upload_list cul on ii.idx = cul.item_id
			JOIN fetching_category fc on (
				SELECT icm.fetching_category_id
				FROM fetching_category fc
				JOIN item_category_map icm on fc.idx = icm.fetching_category_id
				WHERE icm.item_id = ii.idx
					AND fc.fetching_category_name != '기타'
				ORDER BY fc.idx DESC
				LIMIT 1
			) = fc.idx
			WHERE ii.is_verify = 1
				AND cul.is_naver_upload = 1
				AND ii.item_name REGEXP '(?s)^((?![A-Za-z]).)*$'
			LIMIT ${limit}
		`
		const data = await MySQL.execute(query)

		let txt = [`<<<tocnt>>>${data.length}`]

		data.forEach((row) =>
			txt.push(`<<<begin>>>
<<<mapid>>>${row.id}
<<<lprice>>>${row.normal_price}
<<<price>>>${row.price_pc}
<<<mpric>>>${row.price_mobile}
<<<pname>>>${row.title}
<<<pgurl>>>${row.link}
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
		)

		return txt.join('\n')
	}
}

