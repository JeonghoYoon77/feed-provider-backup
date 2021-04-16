import { isEmpty } from 'lodash'

import {MySQL} from '../../utils'

const semiNamePromise = MySQL.execute(`
	SELECT DISTINCT fc.fetching_category_name AS categoryName,
									semi_name AS semiName,
									woman_category IS NOT NULL AS woman,
									man_category IS NOT NULL AS man
	FROM category_semi_name csn
	    JOIN fetching_category fc on csn.man_category = fc.idx OR csn.woman_category = fc.idx
`)

class TSVFormat {
  private readonly _gender: string
	private readonly _id: number | string
	private readonly _shopId: number
	private static _semiNames: { categoryName: string, semiName: string, woman: boolean, man: boolean }[]


	constructor({ itemGender, id, shopId }) {
  	this._gender = itemGender === 'W' ? '여성' : '남성'
		this._id = id
		this._shopId = shopId
	}

	public async title({ mainName, fetchingCategoryName, itemName, customColor, mpn = '' }): Promise<string> {
  	if (!TSVFormat._semiNames?.length) {
  		TSVFormat._semiNames = (await semiNamePromise)
		}

		if ([2, 3, 7, 8, 42, 61, 62].includes(this._shopId) || TSVFormat._semiNames.filter(name => itemName.includes(name.semiName) && fetchingCategoryName.includes(name.categoryName)).length) {
			return `${mainName} ${this._gender} ${itemName} ${`${this.color(customColor)} ${mpn ? mpn : ''}`.trim()}`.trim()
		}
  	return `${mainName} ${this._gender} ${fetchingCategoryName} `
			+ `${itemName} ${`${this.color(customColor)} ${mpn ? mpn : ''}`.trim()}`.trim()
	}

	public pcLink({ cafe24PCAddress }) {
  	return `${cafe24PCAddress}${this._id}`
	}

	public mobileLink({ cafe24MobileAddress }) {
  	return `${cafe24MobileAddress}${this._id}`
	}

	public gender() {
  	return this._gender
	}

	private color(customColor: string) {
		return isEmpty(customColor)
			? ''
			: ' ' + customColor
				.replace(/[^a-zA-Zㄱ-ㅎㅏ-ㅣ가-힣]/gi, ' ')
				.toUpperCase()
				.trim()
	}
}

export default TSVFormat
