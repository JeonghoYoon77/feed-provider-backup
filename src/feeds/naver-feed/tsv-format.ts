import { isEmpty } from 'lodash'

class TSVFormat {
  private gender: string
	

  constructor({ itemGender }) {
  	this.gender = itemGender === 'W' ? '여성' : '남성'
  }
  
  title({
  	mainName,
  	fetchingCategoryName,
  	itemName,
  	customColor,
  }): string {
  	const color = isEmpty(customColor)
  		? ''
  		: ' ' + customColor
  			.replace(/[^a-zA-Z]/gi, ' ')
  			.toUpperCase()

  	return `${mainName} ${this.gender} ${fetchingCategoryName} `
			+ `${itemName}${color}`
  }

  link({ id, cafe24AddressPrefix }) {
  	return `${cafe24AddressPrefix}${id}`
  }
}

export default TSVFormat
