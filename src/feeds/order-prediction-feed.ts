import {Calculate, retry, sleep} from '@fetching-korea/common-utils'
import {GoogleSpreadsheet} from 'google-spreadsheet'
import {parse} from 'json2csv'
import {isDate, isNil, isString, parseInt} from 'lodash'
import {DateTime} from 'luxon'

import sheetData from '../../fetching-sheet.json'

import {MySQL, S3Client} from '../utils'

import {iFeed} from './feed'

export class OrderPredictionFeed implements iFeed {
	async getTsvBuffer(): Promise<Buffer> {
		return Buffer.from(await this.getTsv(), 'utf-8')
	}

	async getTsv(): Promise<string> {
		// 재무 시트
		const doc = new GoogleSpreadsheet(
			'1vXugfbFOQ_aCKtYLWX0xalKF7BJ1IPDzU_1kcAFAEu0'
		)
		const taxDoc = new GoogleSpreadsheet(
			'1SoZM_RUVsuIMyuJdOzWYmwirSb-2c0-5peEPm9K0ATU'
		)
		const targetDoc = new GoogleSpreadsheet(
			'1hmp69Ej9Gr4JU1KJ6iHO-iv1Tga5lMyp8NO5-yRDMlU'
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
		await targetDoc.useServiceAccountAuth({
			client_email: sheetData.client_email,
			private_key: sheetData.private_key,
		})
		/* eslint-enable camelcase */

		await doc.loadInfo()
		await taxDoc.loadInfo()
		await targetDoc.loadInfo()

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
				const value = row['고지 금액'].replace(/,/g, '')
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
                 JSON_ARRAYAGG(CONCAT(si.shop_name, ' ', sp.shop_country))               AS shopName,
                 GROUP_CONCAT(DISTINCT ii.item_name)                                     AS itemName,
                 (SELECT SUM(io.quantity)
                  FROM commerce.shop_order so2
                           JOIN commerce.item_order io2 ON so2.shop_order_number = io2.shop_order_number
                  WHERE so2.fetching_order_number = fo.fetching_order_number
                  GROUP BY so2.fetching_order_number)                                    AS quantity,
                 fo.fetching_order_number,
                 (SELECT JSON_ARRAYAGG(io.item_order_number)) AS itemOrderNumber,
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
                                            and io2.item_order_number = oci2.item_order_number AND
                                           oci2.status = 'ACCEPT'
                              left join commerce.order_return_item ori2
                                        on 1 = 1
                                            and io2.item_order_number = ori2.item_order_number AND
                                           ori2.status = 'ACCEPT'
                     WHERE 1 = 1
                       AND so2.fetching_order_number = fo.fetching_order_number
                 )                                                                       as itemStatusList,
                 ssi.customer_negligence_return_fee                                      AS returnFee,
                 oret.reason_type                                                        AS returnReason,
                 fo.status                                                               AS orderStatus,
                 JSON_ARRAYAGG(scc.name)                                                 AS shippingCompany,
                 JSON_ARRAYAGG(io.invoice)                                               AS invoice,
                 (SELECT JSON_ARRAYAGG(opcl.data)
                  FROM commerce.order_pay_cancel_log opcl
                  WHERE opcl.fetching_order_number = fo.fetching_order_number
                    AND success)                                                         AS refundData,
                 (SELECT JSON_ARRAYAGG(oapcl.data)
                  FROM commerce.order_additional_pay_cancel_log oapcl
                           JOIN commerce.order_additional_pay_log oapl on oapcl.order_additional_pay_log_id = oapl.idx
                           JOIN commerce.order_additional_pay_item oapi
                                ON oapi.additional_item_number = oapl.additional_item_number
                           JOIN commerce.order_additional_pay oap
                                on oap.order_additional_number = oapi.order_additional_number
                  WHERE oap.fetching_order_number = fo.fetching_order_number)            AS additionalRefundData,
                 case
                     when oc.order_cancel_number IS NOT NULL AND (oref.refund_amount < 0 or oref.refund_amount is null)
                         then fo.pay_amount
                     else oref.refund_amount
                     end                                                                 AS refundAmount,
                 fo.pay_amount_detail                                                    AS payAmountDetail,
                 (SELECT JSON_ARRAYAGG(JSON_OBJECT('itemOrderNumber', io2.item_order_number,
                                                   'payAmount', io2.pay_amount_detail,
                                                   'country', sp2.shop_country,
                                                   'isRefunded', ori2.item_order_number IS NOT NULL,
                                                   'vatDeductionRate',
                                                   IF(dm2.vat_deduct && sp2.is_vat_deduction_available,
                                                      sp2.vat_deduction_rate, 0),
                                                   'waypointFeeType', dm2.fee_type,
                                                   'waypointFeeWithDeduction', dm2.fee_with_deduction,
                                                   'waypointFeeWithoutDeduction', dm2.fee_without_deduction,
                                                   'minimumWaypointFeeWithDeduction', dm2.minimum_fee_with_deduction,
                                                   'minimumWaypointFeeWithoutDeduction',
                                                   dm2.minimum_fee_without_deduction
                     ))
                  FROM commerce.item_order io2
                           LEFT JOIN commerce.order_refund_item ori2
                                     ON ori2.item_order_number = io2.item_order_number AND ori2.status = 'ACCEPT' AND
                                        ori2.deleted_at IS NULL
                           JOIN commerce.shop_order so2 ON io2.shop_order_number = so2.shop_order_number
                           JOIN shop_price sp2 ON so2.shop_id = sp2.idx
                           JOIN delivery_method dm2 on so2.delivery_method = dm2.idx
                  WHERE so2.fetching_order_number = fo.fetching_order_number)            AS itemPayAmountDetail,
                 COALESCE(fo.coupon_discount_amount, 0)                                  AS couponDiscountAmount,
                 COALESCE(fo.use_point, 0)                                               as pointDiscountAmount,
                 so.is_ddp_service                                                       AS isDDP,
                 JSON_ARRAYAGG(CONCAT(dm.name, ' ', dm.country))                         AS deliveryMethod
          FROM commerce.fetching_order fo
                   JOIN commerce.shop_order so ON fo.fetching_order_number = so.fetching_order_number
                   JOIN commerce.item_order io on so.shop_order_number = io.shop_order_number
                   LEFT JOIN commerce.order_cancel_item oci on io.item_order_number = oci.item_order_number
                   LEFT JOIN commerce.order_cancel oc on oci.order_cancel_number = oc.order_cancel_number
                   LEFT JOIN commerce.order_return_item oreti on oreti.item_order_number = io.item_order_number
                   LEFT JOIN commerce.order_return oret on oreti.order_return_number = oret.order_return_number
                   LEFT JOIN commerce.order_refund_item orefi on orefi.item_order_number = io.item_order_number
                   LEFT JOIN commerce.order_refund oref on orefi.order_refund_number = oref.order_refund_number
              		 LEFT JOIN commerce.shipping_company_code scc ON scc.code = io.shipping_code
                   JOIN fetching_dev.delivery_method dm ON so.delivery_method = dm.idx
                   JOIN shop_price sp on so.shop_id = sp.idx
                   JOIN fetching_dev.item_info ii ON ii.idx = io.item_id
                   JOIN fetching_dev.shop_info si ON sp.shop_id = si.shop_id
                   LEFT JOIN shop_support_info ssi ON si.shop_id = ssi.shop_id
          WHERE fo.paid_at IS NOT NULL
            AND fo.deleted_at IS NULL
            AND (
                      (fo.created_at + INTERVAL 9 HOUR) >= '2022-09-20'
                  AND
                      (fo.created_at + INTERVAL 9 HOUR) < '2022-09-28'
              )
          GROUP BY fo.fetching_order_number
          ORDER BY fo.created_at ASC
      `
		)

		const feed = data.map((row) => {
			//row.itemOrderNumber = row.itemOrderNumber.map(itemOrder => [itemOrder.itemOrderNumber, itemOrder.orderedAt ? `(${DateTime.fromISO(row.created_at.toISOString()).setZone('Asia/Seoul').toFormat('yyyy-MM-dd HH:mm:ss')})` : ''].join(' ').trim()).join(', ')
			row.itemOrderNumber = row.itemOrderNumber.join(', ')
			const refundData = [...row.refundData ?? [], ...row.additionalRefundData ?? []]
				.map(data => JSON.parse(data)).filter(data => {
					return data?.ResultCode === '2001'
				})

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

			let cardPurchaseValue = cardApprovalNumberList.reduce((acc, e) => {
				const cardApprovalNumber = e.trim()

				const value =
          card[cardApprovalNumber] || cardExtra[cardApprovalNumber] || 0

				return value + acc
			}, 0)

			const itemPriceData: any = {}
			const itemPriceDataCanceled: any = {}
			row.itemPayAmountDetail.map((detail) => {
				const currentItemPriceData: any = {}
				let count = 0
				Object.entries<any>(JSON.parse(detail.payAmount)).forEach(([key, data]) => {
					if (key !== detail.country && (detail.country === 'KR' ? count !== 0 : true)) return
					count++
					data.forEach((row) => {
						if (itemPriceData[row.type]) {
							itemPriceData[row.type] += row.rawValue
						}
						else {
							itemPriceData[row.type] = row.rawValue
							itemPriceDataCanceled[row.type] = 0
						}
						if (currentItemPriceData[row.type]) {
							currentItemPriceData[row.type] += row.rawValue
						}
						else {
							currentItemPriceData[row.type] = row.rawValue
						}
						if (detail.isRefunded) {
							if (itemPriceDataCanceled[row.type])
								itemPriceDataCanceled[row.type] += row.rawValue
							else itemPriceDataCanceled[row.type] = row.rawValue
						}
					})
				})

				let deductedVat
				if (isNil(currentItemPriceData['DEDUCTED_VAT'])) {
					const fasstoPurchaseAmount = currentItemPriceData['SHOP_PRICE_KOR'] + currentItemPriceData['DELIVERY_FEE']
					deductedVat = Calculate.cut(fasstoPurchaseAmount - fasstoPurchaseAmount / (1 + detail.vatDeductionRate))
					currentItemPriceData['DEDUCTED_VAT'] = deductedVat
					if (itemPriceData['DEDUCTED_VAT'])
						itemPriceData['DEDUCTED_VAT'] = deductedVat
					else itemPriceData['DEDUCTED_VAT'] = deductedVat
				}

				if (isNil(currentItemPriceData['WAYPOINT_FEE'])) {
					let waypointFee = 0
					const waypointCurrencyRate = currentItemPriceData['SHOP_PRICE_KOR'] / currentItemPriceData['SHOP_PRICE']

					switch (detail.waypointFeeType) {
					case 'PERCENT':
						waypointFee = Calculate.cut((currentItemPriceData['SHOP_PRICE_KOR'] - deductedVat) * (deductedVat ? detail.waypointFeeWithDeduction : detail.waypointFeeWithoutDeduction))
						break
					case 'FIXED':
						waypointFee = Calculate.cut((deductedVat ? detail.waypointFeeWithDeduction : detail.waypointFeeWithoutDeduction) * waypointCurrencyRate)
						break
					}

					if (deductedVat && detail.waypointMinimumFeeWithDeduction * waypointCurrencyRate > waypointFee) {
						waypointFee = detail.waypointMinimumFeeWithDeduction * waypointCurrencyRate
					} else if (!deductedVat && detail.waypointMinimumFeeWithoutDeduction * waypointCurrencyRate > waypointFee) {
						waypointFee = detail.waypointMinimumFeeWithDeduction * waypointCurrencyRate
					}

					currentItemPriceData['WAYPOINT_FEE'] = waypointFee
					if (itemPriceData['WAYPOINT_FEE'])
						itemPriceData['WAYPOINT_FEE'] = waypointFee
					else itemPriceData['WAYPOINT_FEE'] = waypointFee
				}
			})

			if (
				['DEFECTIVE_PRODUCT', 'WRONG_DELIVERY'].includes(row.returnReason) ||
        !returnCount
			) {
				row.returnFee = 0
			}

			const purchaseValue = itemPriceData['SHOP_PRICE_KOR'] + itemPriceData['DELIVERY_FEE'] - (itemPriceData['DEDUCTED_VAT'] ?? 0) + (row.isDDP ? itemPriceData['DUTY_AND_TAX'] : 0) + (itemPriceData['WAYPOINT_FEE'] ?? 0)
			const lCardRefundValue = itemPriceDataCanceled['SHOP_PRICE_KOR'] + itemPriceDataCanceled['DELIVERY_FEE'] - (itemPriceData['DEDUCTED_VAT'] ?? 0) + (row.isDDP ? itemPriceDataCanceled['DUTY_AND_TAX'] : 0)

			if (row.cardApprovalNumber === '파스토') cardPurchaseValue = purchaseValue

			const data = {
				주문일: DateTime.fromISO(row.created_at.toISOString()).setZone('Asia/Seoul').toFormat('yyyy-MM-dd HH:mm:ss'),
				'주문 상태': status,
				'상품별 주문번호': row.itemOrderNumber,
				'카드 승인번호': row.cardApprovalNumber?.replace('매입', ''),
				'전체 주문번호': row.fetching_order_number,
				'편집샵 매입 주문번호': row.vendorOrderNumber,
				// '카드사': '롯데카드',
				'운송 업체': [...new Set(row.shippingCompany)].join(','),
				'운송장 번호': [...new Set(row.invoice)].join(','),
				'배송 유형': [...new Set(row.deliveryMethod)].join(', '),
				'세금 부과 방식': row.isDDP ? 'DDP' : 'DDU',
				'편집샵명': [...new Set(row.shopName)].join(', '),
				'결제 방식': row.pay_method,
				'상품명': row.itemName,
				'수량': row.quantity,
				'편집샵 결제가': purchaseValue, // cardPurchaseValue,
				'관부가세': (row.isDDP ? 0 : itemPriceData['DUTY_AND_TAX']), // taxTotal
				'운송료': itemPriceData['ADDITIONAL_FEE'] || 0, // waypointDeliveryFee
				'페칭 수수료': itemPriceData['FETCHING_FEE'],
				'쿠폰': row.couponDiscountAmount,
				'적립금': row.pointDiscountAmount,
				'PG수수료': pgFee,
				'실 결제 금액': row.payAmount,
				'결제 환불 금액': refundAmount,
				'PG수수료 환불': refundPgFee,
				// '국내 반품 비용': '',
				// 'IBP 반품 비용': '',
				// '보상 적립금': '',
				// '수선 비용': '',
				'매입 환출 금액': (cancelCount || returnCount) && row.vendorOrderNumber ? Math.abs(lCardRefundValue) : 0, // -cardRefundValue,
			}

			return data
		})

		let targetSheet = targetDoc.sheetsById[2016259526]
		const rows = await targetSheet.getRows()
		// @ts-ignore
		await targetSheet.loadCells()
		let hasModified = false
		for (const i in feed) {
			if (rows[i]) {
				for (const key of Object.keys(feed[i])) {
					if (['예상 배대지 비용'].includes(key)) continue
					if (['실 배대지 비용'].includes(key)) continue
					if (['수동 확인 필요'].includes(feed[i][key])) continue
					if (rows[i][key] === (isString(feed[i][key]) ? feed[i][key] : feed[i][key]?.toString())) continue
					if ((rows[i][key] === (isDate(feed[i][key]) ? feed[i][key]?.toISOString() : feed[i][key]))) continue
					if (!['주문일', '구매확정일'].includes(key) && (parseFloat(rows[i][key]?.replace(/,/g, '')) === (isNaN(parseFloat(feed[i][key])) ? feed[i][key] : parseFloat(feed[i][key])))) continue

					console.log(key, feed[i][key])
					const cell = targetSheet.getCell(rows[i].rowIndex - 1, targetSheet.headerValues.indexOf(key))

					if (cell?.effectiveFormat?.backgroundColor) {
						const {red, green, blue} = cell.effectiveFormat.backgroundColor
						if (!(red === 1 && green === 1 && blue === 1)) continue
					}
					if (cell.effectiveFormat?.numberFormat?.type?.includes('DATE')) cell.effectiveFormat.numberFormat.type = 'TEXT'

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

		return parse(feed, {
			fields: Object.keys(feed[0]),
			delimiter: ',',
			quote: '"',
		})
	}

	async upload() {
		await this.getTsvBuffer()
	}
}
