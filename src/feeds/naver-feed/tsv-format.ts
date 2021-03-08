import { isEmpty } from 'lodash'

class TSVFormat {
  private _gender: string
	private _id: number | string
	

	constructor({ itemGender, id }) {
  	this._gender = itemGender === 'W' ? '여성' : '남성'
		this._id = id
	}
  
	public title({
  	mainName,
  	fetchingCategoryName,
  	itemName,
  	customColor,
	}): string {
  	return `${mainName} ${this._gender} ${fetchingCategoryName} `
			+ `${itemName}${this.color(customColor)}`
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
				.replace(/[^a-zA-Z]/gi, ' ')
				.toUpperCase()
	}
}

export default TSVFormat
