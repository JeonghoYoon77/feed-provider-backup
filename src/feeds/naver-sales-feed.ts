import { iFeed } from './feed'
import { MySQL, S3Client } from '../utils'
import {parse} from 'json2csv'
import moment from 'moment'
import {ES} from '../config'
import {Client} from '@elastic/elasticsearch'


export class NaverSalesFeed implements iFeed {
	client = new Client({
		node: ES.ENDPOINT,
		auth: {
			username: ES.USERNAME,
			password: ES.PASSWORD,
		},
	})

	async upload() {
		const buffer = await this.getTsvBuffer()

		const feedUrl = await S3Client.upload({
			folderName: 'feeds',
			fileName: 'naver-sales-feed.tsv',
			buffer,
		})
		console.log(`FEED_URL: ${feedUrl}`)
	}

	async getTsvBuffer(): Promise<Buffer> {
		return Buffer.from(await this.getTsv(), 'utf-8')
	}

	async getTsv() {
		const target = moment().subtract(2, 'day').format('YYYY-MM-DD')
		const targetEnd = moment().subtract(1, 'day').format('YYYY-MM-DD')

		const body = {
			query: {
				bool: {
					filter: [
						{
							match: {
								type: 'DETAIL',
							},
						},
						{
							match: {
								'inflow.from.channel': 'NAVER',
							},
						},
						{
							match: {
								'inflow.from.type': 'FEED',
							},
						},
						{
							range: {
								createdAt: {
									gte: target,
									lte: targetEnd,
								},
							},
						},
					],
				},
			},
		}

		const esdata = await this.client.search(
			{
				index: 'actions',
				size: 10000,
				body,
			},
			{ requestTimeout: 1000000 }
		)

		console.log(esdata.body.hits.hits.map(row => row._source.item[0].id))

		let data: any = {}

		for (let hit of esdata.body.hits.hits) {
			const query = `
				SELECT DISTINCT ii.idx         AS mall_id,
												1           AS sale_count,
												ip.final_price AS sale_price,
												1           AS order_count,
												?              AS dt
				FROM item_info ii
							 JOIN item_show_price isp on ii.idx = isp.item_id
							 JOIN item_price ip on ii.idx = ip.item_id AND isp.price_rule = ip.price_rule
				WHERE ii.idx = ?
			`
			let [row] = await MySQL.execute(query, [
				targetEnd, hit._source.item[0].id
			])
			console.log(data)
			if (!data[`F${row.mall_id}`]) {
				/* eslint-disable camelcase */
				row.mall_id = `F${row.mall_id}`
				if (!row.sale_count) row.sale_count = 1
				if (!row.order_count) row.order_count = 1
				/* eslint-enable camelcase */

				data[row.mall_id] = row
			} else {
				data[`F${row.mall_id}`].sale_count++
				data[`F${row.mall_id}`].order_count++
			}
		}

		data = Object.values(data)

		return parse(data, {
			fields: Object.keys(data[0]),
			delimiter: '\t',
			quote: '',
		})
	}
}

