import {iFeed} from './feed'
import {MySQL, S3Client} from '../utils'
import {isDate, isEmpty, isNil, isString} from 'lodash'
import {retry, sleep} from '@fetching-korea/common-utils'
import {parse} from 'json2csv'
import {GoogleSpreadsheet} from 'google-spreadsheet'
import sheetData from '../../fetching-sheet.json'

export class CroketFeed implements iFeed {
	async getTsvBuffer(): Promise<Buffer> {
		return Buffer.from(await this.getTsv())
	}

	async getTsvBufferAutoSync(sheetId): Promise<Buffer> {
		return Buffer.from(await this.getTsv(sheetId))
	}

	async getTsv(sheetId: string = null) {
		const data = await MySQL.execute(`
        SELECT DISTINCT bi.brand_name                                     AS '브랜드명',
                        ii.item_name                                      AS '상품명',
                        size.size_name                                    AS '옵션',
                        size.size_quantity                                AS '옵션 재고 수량',
                        COALESCE(iop.final_price, ip.final_price) * 1.01  AS '판매가 (페칭 판매가 + 1%)',
                        CONCAT('https://fetching.co.kr/product/', ii.idx) AS '상품링크'
        FROM item_info ii
                 JOIN item_group_items igi on ii.idx = igi.item_id
                 JOIN item_groups ig on igi.group_id = ig.idx
                 JOIN item_show_price isp on ii.idx = isp.item_id
                 JOIN item_price ip ON isp.item_id = ip.item_id AND isp.price_rule = ip.price_rule
                 JOIN brand_info bi on ii.brand_id = bi.brand_id
                 JOIN item_size size
                      ON isp.item_id = size.item_id AND isp.price_rule = size.price_rule AND size_quantity > 0
                 LEFT JOIN item_option_price iop
                           ON isp.item_id = iop.item_id AND isp.price_rule = iop.price_rule AND
                              size.size_name = iop.option_name
        WHERE ig.name LIKE '[크로켓]%'
          AND ig.is_active
          AND ii.is_sellable
		`)

		const targetDoc = new GoogleSpreadsheet(
			'1U7DVIkd6BOplFpeD1qzQfBXWNXgvCR_Y3zbo_vRg62w'
		)

		/* eslint-disable camelcase */
		await targetDoc.useServiceAccountAuth({
			client_email: sheetData.client_email,
			private_key: sheetData.private_key,
		})
		/* eslint-enable camelcase */

		await targetDoc.loadInfo()

		let targetSheet = targetDoc.sheetsById[sheetId]
		const rows = await targetSheet.getRows()
		// @ts-ignore
		await targetSheet.loadCells()
		let hasModified = false
		for (const i in data) {
			data[i]['번호'] = parseInt(i) + 1
			if (rows[i]) {
				for (const key of Object.keys(data[i])) {
					const cell = targetSheet.getCell(rows[i].rowIndex - 1, targetSheet.headerValues.indexOf(key))

					if (rows[i][key] === (isString(data[i][key]) ? data[i][key] : data[i][key]?.toString())) continue

					cell.value = data[i][key]
					hasModified = true
				}
			} else {
				await retry(3, 3000)(async () => {
					await targetSheet.addRow(data[i], {raw: true, insert: true})
				})
				await sleep(500)
			}
		}
		if (hasModified) {
			await retry(3, 3000)(async () => {
				await targetSheet.saveUpdatedCells()
			})
		}

		return parse(data, {
			fields: Object.keys(data[0]),
			delimiter: ',',
			quote: '"',
		})
	}

	async upload() {
		console.log(
			await S3Client.upload({
				folderName: 'feeds',
				fileName: 'croket.csv',
				buffer: await this.getTsvBufferAutoSync(
					'1220541461'
				),
				contentType: 'text/csv',
			})
		)
	}
}
