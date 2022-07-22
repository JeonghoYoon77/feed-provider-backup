import {retry, sleep} from '@fetching-korea/common-utils'
import {GoogleSpreadsheet} from 'google-spreadsheet'
import {parse} from 'json2csv'
import {isDate, isString, parseInt} from 'lodash'
import {DateTime} from 'luxon'

import sheetData from '../../fetching-sheet.json'

import {MySQL, S3Client} from '../utils'

import {iFeed} from './feed'

export class OrderFeed implements iFeed {
	async getTsvBuffer(): Promise<Buffer> {
		return Buffer.from(await this.getTsv({start: null, end: null}), 'utf-8')
	}

	async getTsvBufferWithRange(start: Date, end: Date, targetSheetId = null): Promise<Buffer> {
		return Buffer.from(await this.getTsv({start, end, targetSheetId}), 'utf-8')
	}

	async getTsv({start, end, targetSheetId}: { start: Date, end: Date, targetSheetId?: string }): Promise<string> {
		// 재무 시트
		const doc = new GoogleSpreadsheet(
			'1vXugfbFOQ_aCKtYLWX0xalKF7BJ1IPDzU_1kcAFAEu0'
		)
		const taxDoc = new GoogleSpreadsheet(
			'1SoZM_RUVsuIMyuJdOzWYmwirSb-2c0-5peEPm9K0ATU'
		)

		/* eslint-disable camelcase */
		await doc.useServiceAccountAuth({
			client_email: sheetData.client_email,
			private_key: sheetData.private_key,
		})
		await taxDoc.useServiceAccountAuth({
			client_email: sheetData.client_email,
			private_key: sheetData.private_key,
		})
		/* eslint-enable camelcase */

		await doc.loadInfo()
		await taxDoc.loadInfo()

		const eldexSheet = doc.sheetsByTitle['엘덱스비용'] // 엘덱스비용
		const cardSheet = doc.sheetsByTitle['롯데카드이용내역'] // 롯데카드이용내역 (정확하지만 한달에 한번씩만 갱신)
		const cardExtraSheet = doc.sheetsByTitle['롯데카드승인내역'] // 롯데카드승인내역 (부정확하지만 자주 갱신)
		const taxSheet = taxDoc.sheetsById['1605798118']

		const eldexRaw = await eldexSheet.getRows()
		const cardRaw = await cardSheet.getRows()
		const cardExtraRaw = await cardExtraSheet.getRows()
		const taxRaw = await taxSheet.getRows()

		const eldex = {}
		const card = {}
		const cardExtra = {}
		const cardRefund = {}
		const tax = {}

		eldexRaw.forEach((row) => {
			const id = row['송장번호']
			const value = row['2차결제금액(원)'].replace(/,/g, '')
			eldex[id] = parseInt(value)
		})

		taxRaw.forEach((row) => {
			if (row['주문번호']) {
				const id = row['주문번호'].trim()
				const value = row['금액'].replace(/,/g, '')
				tax[id] = parseInt(value)
			}
		})

		cardRaw.forEach((row) => {
			const id = row['승인번호'].trim()
			const value = parseInt(row['청구금액'].replace(/,/g, ''))
			const refundValue = parseInt(row['전월취소및부분취소'].replace(/,/g, ''))

			cardRefund[id] = 0

			if (value < 0) {
				cardRefund[id] = value
			} else if (value > 0) {
				card[id] = value
			}

			if (refundValue < 0) {
				cardRefund[id] = refundValue
			}
		})

		cardExtraRaw.forEach((row) => {
			const id = row['승인번호'].trim()
			const value = parseInt(row['승인금액(원화)'].replace(/,/g, ''))
			const isCanceled = row['취소여부'] === 'Y'

			if (value > 0) {
				if (isCanceled) {
					cardRefund[id] = -value
				} else {
					cardExtra[id] = value
				}
			}

			if (cardRefund[id] === 0 && cardRefund[id] === undefined) {
				cardRefund[id] = 0

				if (value < 0) {
					cardRefund[id] = value
				}
			}
		})

		const data = await MySQL.execute(
			`
				SELECT fo.created_at,
							 COALESCE(io.ordered_at, (SELECT fom2.created_at
																				FROM commerce.fetching_order_memo fom2
																				WHERE so.shop_order_number = fom2.shop_order_number
																					AND to_value = 'ORDER_COMPLETE'
																				ORDER BY fom2.created_at DESC
																				LIMIT 1))                                         orderedAt,
							 so.shipping_completed_at + INTERVAL 1 WEEK                              as completedAt,
							 od.recipient_name                                                       as name,
							 od.phone_number                                                         as phone,
							 JSON_ARRAYAGG(CONCAT(si.shop_name, ' ', sp.shop_country))                    AS shopName,
							 GROUP_CONCAT(DISTINCT ii.item_name)                                     AS itemName,
							 (SELECT SUM(io.quantity)
								FROM commerce.shop_order so2
											 JOIN commerce.item_order io2 ON so2.shop_order_number = io2.shop_order_number
								WHERE so2.fetching_order_number = fo.fetching_order_number
								GROUP BY so2.fetching_order_number)                                    AS quantity,
							 fo.fetching_order_number,
							 (SELECT JSON_ARRAYAGG(JSON_OBJECT('itemOrderNumber', io.item_order_number, 'orderedAt',
																								 COALESCE(io.ordered_at, (SELECT fom2.created_at
																																					FROM commerce.fetching_order_memo fom2
																																					WHERE so.shop_order_number = fom2.shop_order_number
																																						AND to_value = 'ORDER_COMPLETE'
																																					ORDER BY fom2.created_at DESC
																																					LIMIT 1))))) AS itemOrderNumber,
							 GROUP_CONCAT(DISTINCT so.vendor_order_number separator ', ')            AS vendorOrderNumber,
							 GROUP_CONCAT(DISTINCT so.card_approval_number separator ', ')           AS cardApprovalNumber,
							 fo.pay_amount                                                           AS payAmount,
							 (
								 SELECT JSON_ARRAYAGG(JSON_OBJECT('method', oapi.pay_method, 'amount', oapi.amount))
								 FROM commerce.order_additional_pay oap
												JOIN commerce.order_additional_pay_item oapi
														 ON oapi.order_additional_number = oap.order_additional_number AND
																oapi.status = 'PAID'
								 WHERE oap.fetching_order_number = fo.fetching_order_number
							 )                                                                       AS additionalPayInfo,
							 fo.status,
							 fo.order_path,
							 fo.pay_method,
							 (
								 SELECT JSON_ARRAYAGG(
													case
														when ori2.return_item_number is not null
															then '반품'
														when oci2.cancel_item_number is not null
															then '주문 취소'
														when fo.status = 'COMPLETE'
															then '구매 확정'
														when so.status not in
																 ('BEFORE_DEPOSIT', 'ORDER_AVAILABLE', 'ORDER_WAITING',
																	'PRE_ORDER_REQUIRED',
																	'ORDER_DELAY')
															then '주문 완료'
														else ''
														end
													)
								 FROM commerce.item_order io2
												inner join commerce.shop_order so2
																	 on 1 = 1
																		 and so2.shop_order_number = io2.shop_order_number
												left join commerce.order_cancel_item oci2
																	on 1 = 1
																		and io2.item_order_number = oci2.item_order_number
												left join commerce.order_return_item ori2
																	on 1 = 1
																		and io2.item_order_number = ori2.item_order_number
								 WHERE 1 = 1
									 AND so2.fetching_order_number = fo.fetching_order_number
							 )                                                                       as itemStatusList,
							 ssi.customer_negligence_return_fee                                      AS returnFee,
							 oret.reason_type                                                        AS returnReason,
							 fo.status                                                               AS orderStatus,
							 so.status                                                               AS shopStatus,
							 io.invoice                                                              AS invoice,
							 (SELECT JSON_ARRAYAGG(opcl.data)
								FROM commerce.order_pay_cancel_log opcl
								WHERE opcl.fetching_order_number = fo.fetching_order_number
									AND success)                                                         AS refundData,
							 (SELECT JSON_ARRAYAGG(oapcl.data)
								FROM commerce.order_additional_pay_cancel_log oapcl
											 JOIN commerce.order_additional_pay_log oapl on oapcl.order_additional_pay_log_id = oapl.idx
								    JOIN commerce.order_additional_pay_item oapi ON oapi.additional_item_number = oapl.additional_item_number
											 JOIN commerce.order_additional_pay oap on oap.order_additional_number = oapi.order_additional_number
								WHERE oap.fetching_order_number = fo.fetching_order_number)            AS additionalRefundData,
							 case
								 when oc.order_cancel_number IS NOT NULL AND (oref.refund_amount < 0 or oref.refund_amount is null)
									 then fo.pay_amount
								 else oref.refund_amount
								 end                                                                   AS refundAmount,
							 fo.pay_amount_detail                                                    AS payAmountDetail,
							 (SELECT JSON_ARRAYAGG(JSON_OBJECT('itemOrderNumber', io2.item_order_number, 'payAmount',
																								 io2.pay_amount_detail, 'country', sp2.shop_country, 'isRefunded',
																								 ori2.item_order_number IS NOT NULL))
								FROM commerce.item_order io2
											 LEFT JOIN commerce.order_refund_item ori2
																 ON ori2.item_order_number = io2.item_order_number AND ori2.status = 'ACCEPT' AND
																		ori2.deleted_at IS NULL
											 JOIN commerce.shop_order so2 ON io2.shop_order_number = so2.shop_order_number
											 JOIN shop_price sp2 ON so2.shop_id = sp2.idx
								WHERE so2.fetching_order_number = fo.fetching_order_number)            AS itemPayAmountDetail,
							 COALESCE(fo.coupon_discount_amount, 0)                                  AS couponDiscountAmount,
							 COALESCE(fo.use_point, 0)                                               as pointDiscountAmount,
							 exists(
								 select 1
								 from commerce.order_refund refund
								 where 1 = 1
									 and refund.status = 'ACCEPT'
									 and refund.fetching_order_number = fo.fetching_order_number
								 )                                                                     AS taxRefunded,
							 so.is_ddp_service                                                       AS isDDP,
							 weight                                                                  AS weight,
							 JSON_ARRAYAGG(CONCAT(dm.name, ' ', dm.country))                         AS deliveryMethod,
							 (SELECT u.name
								FROM commerce.fetching_order_memo fom
											 JOIN fetching_dev.users u ON fom.admin_id = u.idx
								WHERE fom.fetching_order_number = fo.fetching_order_number
								ORDER BY fom.to_value = 'ORDER_COMPLETE' DESC, fom.created_at DESC
								LIMIT 1)                                                               AS assignee
				FROM commerce.fetching_order fo
							 JOIN commerce.shop_order so ON fo.fetching_order_number = so.fetching_order_number
							 JOIN commerce.item_order io on so.shop_order_number = io.shop_order_number
							 LEFT JOIN commerce.order_cancel_item oci on io.item_order_number = oci.item_order_number
							 LEFT JOIN commerce.order_cancel oc on oci.order_cancel_number = oc.order_cancel_number
							 LEFT JOIN commerce.order_exchange_item oei on oei.item_order_number = io.item_order_number
							 LEFT JOIN commerce.order_exchange oe on oei.order_exchange_number = oe.order_exchange_number
							 LEFT JOIN commerce.order_return_item oreti on oreti.item_order_number = io.item_order_number
							 LEFT JOIN commerce.order_return oret on oreti.order_return_number = oret.order_return_number
							 LEFT JOIN commerce.order_refund_item orefi on orefi.item_order_number = io.item_order_number
							 LEFT JOIN commerce.order_refund oref on orefi.order_refund_number = oref.order_refund_number
							 LEFT JOIN commerce.order_delivery od ON od.fetching_order_number = fo.fetching_order_number
							 JOIN commerce.user u on fo.user_id = u.idx
							 JOIN fetching_dev.delivery_method dm ON so.delivery_method = dm.idx
							 LEFT JOIN commerce.shop_order_weight sow ON so.shop_order_number = sow.shop_order_number
							 JOIN shop_price sp on so.shop_id = sp.idx
							 JOIN fetching_dev.item_info ii ON ii.idx = io.item_id
							 JOIN fetching_dev.shop_info si ON sp.shop_id = si.shop_id
							 LEFT JOIN shop_support_info ssi ON si.shop_id = ssi.shop_id
				WHERE fo.paid_at IS NOT NULL
					AND fo.deleted_at IS NULL
					AND (
							(fo.created_at + INTERVAL 9 HOUR) >= ?
						AND
							(fo.created_at + INTERVAL 9 HOUR) < ?
					)
				GROUP BY fo.fetching_order_number
				ORDER BY fo.created_at ASC
      `,
			[start, end]
		)

		const [{currencyRate}] = await MySQL.execute(`
        SELECT currency_rate as currencyRate
        FROM currency_info
        WHERE currency_tag = 'EUR'
    `)

		const feed = data.map((row) => {
			row.itemOrderNumber = row.itemOrderNumber.map(itemOrder => [itemOrder.itemOrderNumber, itemOrder.orderedAt ? `(${DateTime.fromISO(row.created_at.toISOString()).setZone('Asia/Seoul').toFormat('yyyy-MM-dd HH:mm:ss')})` : ''].join(' ').trim()).join(', ')
			const refundData = [...row.refundData ?? [], ...row.additionalRefundData ?? []]
				.map(data => JSON.parse(data)).filter(data => {
					return data?.ResultCode === '2001'
				})
			if (row.fetching_order_number === '20220206-0000021') {
				console.log(row.refundData, row.additionalRefundData)
			}
			const refundAmount = refundData.reduce(((a: number, b: any) => a + parseInt(b.CancelAmt)), 0)
			// const salesAmount = row.payAmount - refundAmount
			// const totalPrice = priceData.SHOP_PRICE_KOR + priceData.DUTY_AND_TAX + priceData.DELIVERY_FEE
			// const totalTotalPrice = !row.refundAmount ? (totalPrice + pgFee - refundAmount) : 0

			// const profit = salesAmount - totalTotalPrice

			let settleCount = 0
			let completeCount = 0
			let cancelCount = 0
			let returnCount = 0

			// '반품', '주문 취소', '구매 확정', '주문 완료'
			for (const status of row.itemStatusList) {
				if (status === '구매 확정') settleCount++
				if (status === '주문 완료') completeCount++
				if (status === '주문 취소') cancelCount++
				if (status === '반품') returnCount++
			}

			let status = ''
			if (!settleCount && !completeCount && cancelCount && !returnCount) status = '주문 취소'
			else if (!settleCount && completeCount && !cancelCount && !returnCount) status = '주문 완료'
			else if (settleCount && !completeCount && !cancelCount && !returnCount) status = '구매 확정'
			else if (!settleCount && !completeCount && !cancelCount && returnCount) status = '반품'
			else if (!settleCount && completeCount && cancelCount && !returnCount) status = '주문 완료, 일부 취소'
			else if (settleCount && !completeCount && cancelCount && !returnCount) status = '구매 확정, 일부 취소'
			else if (!settleCount && completeCount && !cancelCount && returnCount) status = '주문 완료, 일부 반품'
			else if (settleCount && !completeCount && !cancelCount && returnCount) status = '구매 확정, 일부 반품'

			const priceData: any = {}
			JSON.parse(row.payAmountDetail).forEach((row) => {
				if (priceData[row.type]) {
					priceData[row.type] += row.value
				} else {
					priceData[row.type] = row.value
				}
			})

			const remarks = []

			let pgFee = 0

			if (row.pay_method === 'CARD') {
				pgFee = Math.round(row.payAmount * 0.014)
			} else if (row.pay_method === 'KAKAO') {
				pgFee = Math.round(row.payAmount * 0.015)
			} else if (row.pay_method === 'NAVER') {
				pgFee = Math.round(row.payAmount * 0.015)
			} else if (row.pay_method === 'ESCROW') {
				pgFee = Math.round(row.payAmount * 0.017)
			} else if (row.pay_method === 'ESCROW_CARD') {
				pgFee = Math.round(row.payAmount * 0.016)
			}

			if (row.additionalPayInfo) {
				for (const {amount, method} of row.additionalPayInfo) {
					row.payAmount += amount
					if (method === 'CARD') pgFee += Math.round(amount * 0.014)
					else if (method === 'KAKAO') pgFee += Math.round(amount * 0.015)
					else if (method === 'NAVER') pgFee += Math.round(amount * 0.015)
					else if (method === 'ESCROW') pgFee += Math.round(amount * 0.017)
					else if (method === 'ESCROW_CARD')
						pgFee += Math.round(amount * 0.016)
				}
			}

			const refundPgFee = refundData.reduce((a, b) => {
				const amount = parseInt(b.CancelAmt)
				let pgFee = 0
				if (b.PayMethod === 'CARD') pgFee = Math.round(amount * 0.014)
				else if (b.PayMethod === 'KAKAO') pgFee = Math.round(amount * 0.015)
				else if (b.PayMethod === 'NAVER') pgFee = Math.round(amount * 0.015)
				else if (b.PayMethod === 'ESCROW') pgFee = Math.round(amount * 0.017)
				else if (b.PayMethod === 'ESCROW_CARD')
					pgFee = Math.round(amount * 0.016)
				return a + pgFee
			}, 0)

			const cardApprovalNumberList =
        row.cardApprovalNumber?.split(',') ?? []

			const cardRefundValue = cardApprovalNumberList.reduce((acc, e) => {
				const cardApprovalNumber = e.trim()

				const refund = cardRefund[cardApprovalNumber] || 0

				return refund + acc
			}, 0)

			let cardPurchaseValue = cardApprovalNumberList.reduce((acc, e) => {
				const cardApprovalNumber = e.trim()

				const value =
          card[cardApprovalNumber] || cardExtra[cardApprovalNumber] || 0

				return value + acc
			}, 0)

			const itemPriceData: any = {}
			const itemPriceDataCanceled: any = {}
			row.itemPayAmountDetail.map((detail) => {
				let count = 0
				return Object.entries<any>(JSON.parse(detail.payAmount)).forEach(([key, data]) => {
					if (key !== detail.country && (detail.country === 'KR' ? count !== 0 : true)) return
					count++
					data.forEach((row) => {
						if (itemPriceData[row.type])
							itemPriceData[row.type] += row.rawValue
						else {
							itemPriceData[row.type] = row.rawValue
							itemPriceDataCanceled[row.type] = 0
						}
						if (detail.isRefunded) {
							if (itemPriceDataCanceled[row.type])
								itemPriceDataCanceled[row.type] += row.rawValue
							else itemPriceDataCanceled[row.type] = row.rawValue
						}
					})
				})
			})

			if (
				['DEFECTIVE_PRODUCT', 'WRONG_DELIVERY'].includes(row.returnReason) ||
        !returnCount
			) {
				row.returnFee = 0
			}

			if (row.cardApprovalNumber?.includes('매입')) {
				remarks.push('매입')
			}

			// 매입환출 완료여부
			let purchaseReturn = '환출미완료'

			if (
				(row.itemStatusList?.includes('주문 취소') ||
          row.itemStatusList?.includes('반품')) &&
        cardPurchaseValue !== 0
			) {
				if (cardRefundValue < 0) {
					purchaseReturn = '환출완료'
				}
			} else {
				purchaseReturn = '해당없음'
			}

			const taxTotal = tax[row.fetching_order_number] || 0
			let taxRefund: any = 0

			if (row.taxRefunded == 1) {
				taxRefund = taxTotal
				if (status.includes('일부')) taxRefund = '수동 확인 필요'
			}

			const purchaseValue = itemPriceData['SHOP_PRICE_KOR'] + itemPriceData['DELIVERY_FEE'] + (row.isDDP ? itemPriceData['DUTY_AND_TAX'] : 0) + (itemPriceData['WAYPOINT_FEE'] ?? 0)
			const lCardRefundValue = itemPriceDataCanceled['SHOP_PRICE_KOR'] + itemPriceDataCanceled['DELIVERY_FEE'] + (row.isDDP ? itemPriceDataCanceled['DUTY_AND_TAX'] : 0)

			if (row.cardApprovalNumber === '파스토') cardPurchaseValue = purchaseValue

			let waypointDeliveryFee = eldex[row.invoice] ?? 0

			if (row.weight) {
				waypointDeliveryFee = ((row.weight < 1 ? 3 + 3.2 + 1.5 : 3 * row.weight + 3.2 + 1.5) * currencyRate).toFixed(3)
			}

			const data = {
				주문일: DateTime.fromISO(row.created_at.toISOString()).setZone('Asia/Seoul').toFormat('yyyy-MM-dd HH:mm:ss'),
				상태: status,
				'배송 유형': [...new Set(row.deliveryMethod)].join(', '),
				구매확정일: row.completedAt ? DateTime.fromISO(row.completedAt.toISOString()).setZone('Asia/Seoul').toFormat('yyyy-MM-dd HH:mm:ss') : null,
				편집샵명: [...new Set(row.shopName)].join(', '),
				상품명: row.itemName,
				수량: row.quantity,
				주문번호: row.fetching_order_number,
				'상품별 주문번호': row.itemOrderNumber,
				'편집샵 주문번호': row.vendorOrderNumber,
				'카드 승인번호': row.cardApprovalNumber?.replace('매입', ''),
				'결제 방식': row.pay_method,
				'페칭 판매가': priceData['ORIGIN_PRICE'] + (row.isDDP ? itemPriceData['DUTY_AND_TAX'] : 0),
				'페칭 수수료': itemPriceData['FETCHING_FEE'],
				'롯데카드 캐시백': ((cardPurchaseValue - cardRefundValue) * 0.025).toFixed(3),
				쿠폰: row.couponDiscountAmount,
				적립금: row.pointDiscountAmount,
				결제가: row.payAmount,
				환불금액: refundAmount,
				'PG수수료': pgFee,
				'운송장번호': row.invoice,
				'예상 배대지 비용': itemPriceData['ADDITIONAL_FEE'] || 0,
				'실 배대지 비용': waypointDeliveryFee,
				'예상 관부가세': (row.isDDP ? 0 : itemPriceData['DUTY_AND_TAX']),
				'납부 관부가세': taxTotal,
				'관부가세 환급': taxRefund,
				'PG수수료 환불': refundPgFee,
				'예상 매입 금액': purchaseValue,
				'실 매입 금액': cardPurchaseValue,
				'예상 매입환출금': (cancelCount || returnCount) && row.vendorOrderNumber ? (completeCount && settleCount) ? lCardRefundValue : -cardPurchaseValue : 0,
				'실 매입환출금액': -cardRefundValue,
				반품수수료: row.returnFee || 0,
				비고: remarks.join(', '),
				'발주 담당자': row.assignee,
				// 주문자: row.name,
				// 전화번호: decryptInfo(row.phone) + ' ',
				// '매입환출 완료여부': purchaseReturn,
			}

			return data
		})

		if (targetSheetId) {
			let targetSheet = doc.sheetsById[targetSheetId]
			const rows = await targetSheet.getRows()
			// @ts-ignore
			await targetSheet.loadCells()
			let hasModified = false
			for (const i in feed) {
				if (rows[i]) {
					for (const key of Object.keys(feed[i])) {
						if (!feed[i][key] && !['예상 배대지 비용'].includes(key)) continue
						if (rows[i][key] && ['실 배대지 비용'].includes(key)) continue
						if (rows[i][key] && ['수동 확인 필요'].includes(feed[i][key])) continue
						if (rows[i][key] === (isString(feed[i][key]) ? feed[i][key] : feed[i][key]?.toString())) continue
						if ((rows[i][key] === (isDate(feed[i][key]) ? feed[i][key]?.toISOString() : feed[i][key]))) continue
						if (!['주문일', '구매확정일'].includes(key) && (parseFloat(rows[i][key]?.replace(/,/g, '')) === (isNaN(parseFloat(feed[i][key])) ? feed[i][key] : parseFloat(feed[i][key])))) continue

						const cell = targetSheet.getCell(rows[i].rowIndex - 1, targetSheet.headerValues.indexOf(key))
						const {red, green, blue} = cell.effectiveFormat.backgroundColor
						if (!(red === 1 && green === 1 && blue === 1)) continue
						if (cell.effectiveFormat.numberFormat?.type.includes('DATE')) cell.effectiveFormat.numberFormat.type = 'TEXT'

						cell.value = feed[i][key]
						hasModified = true
					}
				} else {
					await retry(3, 3000)(async () => {
						await targetSheet.addRow(feed[i], {raw: true, insert: true})
					})
					await sleep(500)
				}
			}
			if (hasModified) {
				await retry(3, 3000)(async () => {
					await targetSheet.saveUpdatedCells()
				})
			}
		}

		return parse(feed, {
			fields: Object.keys(feed[0]),
			delimiter: ',',
			quote: '"',
		})
	}

	async upload() {
		/*console.log(
			await S3Client.upload({
				folderName: 'feeds',
				fileName: '1월.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2022-01-01T00:00:00.000Z'),
					new Date('2022-02-01T00:00:00.000Z'),
					'1211460681'
				),
				contentType: 'text/csv',
			})
		)*/

		console.log(
			await S3Client.upload({
				folderName: 'feeds',
				fileName: '2월.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2022-02-01T00:00:00.000Z'),
					new Date('2022-03-01T00:00:00.000Z'),
					'65319871'
				),
				contentType: 'text/csv',
			})
		)
		return
		console.log(
			await S3Client.upload({
				folderName: 'feeds',
				fileName: '3월.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2022-03-01T00:00:00.000Z'),
					new Date('2022-04-01T00:00:00.000Z'),
					'1595058800'
				),
				contentType: 'text/csv',
			})
		)

		console.log(
			await S3Client.upload({
				folderName: 'feeds',
				fileName: '4월.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2022-04-01T00:00:00.000Z'),
					new Date('2022-05-01T00:00:00.000Z'),
					'513874448'
				),
				contentType: 'text/csv',
			})
		)

		console.log(
			await S3Client.upload({
				folderName: 'feeds',
				fileName: '5월.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2022-05-01T00:00:00.000Z'),
					new Date('2022-06-01T00:00:00.000Z'),
					'738638695'
				),
				contentType: 'text/csv',
			})
		)

		console.log(
			await S3Client.upload({
				folderName: 'feeds',
				fileName: '6월.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2022-06-01T00:00:00.000Z'),
					new Date('2022-07-01T00:00:00.000Z'),
					'806883469'
				),
				contentType: 'text/csv',
			})
		)

		console.log(
			await S3Client.upload({
				folderName: 'feeds',
				fileName: '7월.csv',
				buffer: await this.getTsvBufferWithRange(
					new Date('2022-07-01T00:00:00.000Z'),
					new Date('2022-08-01T00:00:00.000Z'),
					'2046794903'
				),
				contentType: 'text/csv',
			})
		)
	}
}
