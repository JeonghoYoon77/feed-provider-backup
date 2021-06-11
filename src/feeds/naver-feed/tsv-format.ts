import { isEmpty } from 'lodash'

class TSVFormat {
  private readonly _gender: string
	private readonly _id: number | string
	private readonly _shopId: number


	constructor({ itemGender, id, shopId }) {
  	this._gender = itemGender === 'W' ? '여성' : '남성'
		this._id = id
		this._shopId = shopId
	}

	public async title({ itemCode, mainName, itemName, customColor, mpn = '' }): Promise<string> {

		itemName = itemName.trim()
  	let title = `${mainName} ${this._gender} ${itemName} ${`${mpn ? mpn : itemCode} ${this.color(customColor)}`.trim()}`.trim()

		title = title.replace('é', '')
		title = title.split('\n').join('')

		return title.replace(/([&"'_])/g, '').split(' ').filter(data => data).join(' ')
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

	public searchTag({itemName, brandMainName, categoryName2, categoryName3}) {
  	itemName = itemName.trim()
  	const tags = [
  		`${brandMainName}${itemName}`,
			`${brandMainName}${this._gender}${categoryName3}`,
			`${brandMainName}${this._gender}${categoryName2}`,
			`${brandMainName}${categoryName3}`,
			`${brandMainName}${categoryName2}`,
			`${this._gender}명품${categoryName3}`,
			`${this._gender}명품${categoryName2}`,
			`${this._gender}${categoryName3}`,
			`${this._gender}${categoryName2}`,
		]
		return tags.join('|').split(' ').join('')
	}
}

export default TSVFormat
