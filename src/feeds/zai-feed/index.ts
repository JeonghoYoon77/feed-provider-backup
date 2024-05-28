import * as fs from 'fs'
import { parse } from 'json2csv'
import {chunk} from 'lodash'
import { format } from 'mysql2'
import mysql from 'mysql2/promise'

import { MySQL } from '../../utils'
import { S3Client } from '../../utils'

import Constants from '../naver-feed/constants'
import TSVData from '../naver-feed/tsv-data'
import TSVFormat from '../naver-feed/tsv-format'
import { iFeed } from '../feed'
import { Worker, isMainThread } from 'worker_threads';
import {
	selectSellableItems,
	selectZaiFeed
} from './workers/zaiFeedProcessor'

const constants = new Constants()

export class ZaiFeed implements iFeed {
	static brandSemiNameMap: any
	static categorySemiNameMap: any

	async upload(chunkSize: number = 1000) {

		if (isMainThread) {
			
		} else {

		}
		const readStream = await this.getTsvBuffer(chunkSize);

		// const feedUrl = await S3Client.upload({
		// 	folderName: 'feeds',
		// 	fileName: 'zai-feed.tsv',
		// 	readStream,
		// })

		// console.log('FEED_URL\t:', feedUrl)
	}

	async getTsvBuffer(chunkSize: number = 1000, delimiter = '\t'): Promise<fs.ReadStream> {
		return this.getTsv(chunkSize, delimiter)
	}

	countSellableItems(): string {
		return `
			select 
				count(*) as cnt
			from
				item_info
			where
				is_sellable
		`
	}

	spawnWorkerThread(
		offset: number,
		chunkSize: number,
		brandNameMap: Object,
		categoryNameMap: Object
	): Worker {
		const worker: Worker = new Worker(
			'./workers/zaiFeedProcessor.js',
			{
				workerData: {
					offset: offset, 
					chunkSize: chunkSize, 
					brandNameMap: brandNameMap, 
					categoryNameMap: categoryNameMap
				}
			}
		).on('message', (result: TSVData[]) => {
			console.log('Result from worker:', result);
		}).on('error', (msg) => {
			console.log(`Error from worker thread: ${msg}`);
		})
		return worker;
	}

	async getTsv(chunkSize: number = 1000, delimiter = '\t'): Promise<fs.ReadStream> {
		try {
			fs.unlinkSync('./zai-feed.tsv')
		} catch {}
		fs.writeFileSync('./zai-feed.tsv', '')
		const brandSemiNameRaw = await MySQL.execute('SELECT brand_id AS brandId, JSON_ARRAYAGG(semi_name) AS semiName FROM brand_search_name GROUP BY brand_id')
		const categorySemiNameRaw = await MySQL.execute('SELECT category AS categoryId, JSON_ARRAYAGG(semi_name) AS semiName FROM category_semi_name GROUP BY category')
		ZaiFeed.brandSemiNameMap = Object.fromEntries(brandSemiNameRaw.map(row => [row.brandId, row.semiName]))
		ZaiFeed.categorySemiNameMap = Object.fromEntries(categorySemiNameRaw.map(row => [row.categoryId, row.semiName]))
		const sellableItemsCount = (await MySQL.execute(this.countSellableItems()))[0]['cnt'];
		const sellableItemsChunked: mysql.RowDataPacket[][] = [];
		for (let i: number = 0; i <= 5000; i = i + chunkSize) {
			// TODO: worker threads 적용
			// Create a new worker thread
			const worker = this.spawnWorkerThread(i, chunkSize, ZaiFeed.brandSemiNameMap, ZaiFeed.categorySemiNameMap);




			// let sellableItemsChunk = (
			// 	await MySQL.execute(this.selectSellableItemsPaged({offset: i, limit: chunkSize}))
			// ).map((row: mysql.RowDataPacket) => row['idx']);
			// sellableItemsChunked.push(sellableItemsChunk);
			// const data = await MySQL.execute(ZaiFeed.query(sellableItemsChunk));
			// const currentData = data.filter((row: mysql.RowDataPacket) => row.option_detail && row.category_name1 && row.category_name2 && row.category_name3);
			// const tsvData: TSVData[] = (await Promise.all(currentData.map(ZaiFeed.makeRow))).filter(row => row);
			// fs.appendFileSync('./zai-feed.tsv', parse(tsvData, {
			// 	fields: Object.keys(tsvData[0]),
			// 	header: i === 0,
			// 	delimiter,
			// 	quote: ''
			// }))
		}
		return fs.createReadStream('./zai-feed.tsv')
	}
}