import { isEmpty } from 'lodash'

import {MySQL} from '../../utils'

const semiNamePromise = MySQL.execute(`
	SELECT semi_name AS semiName,
	       woman_category IS NOT NULL AS woman,
	       man_category IS NOT NULL AS man
	FROM category_semi_name
`)

class TSVFormat {
  private readonly _gender: string
	private readonly _id: number | string
	private static _semiNames: { semiName: string, woman: boolean, man: boolean }[]


	constructor({ itemGender, id }) {
  	this._gender = itemGender === 'W' ? '여성' : '남성'
		this._id = id
	}

	public async title({ mainName, fetchingCategoryName, itemName, customColor, mpn = '' }): Promise<string> {
  	if (!TSVFormat._semiNames) TSVFormat._semiNames = (await semiNamePromise)

		if (TSVFormat._semiNames.filter(name => itemName.includes(name)).length) {
			console.log(TSVFormat._semiNames.filter(name => itemName.includes(name.semiName) && (this._gender === '남성' ? name.man : name.woman)))
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
