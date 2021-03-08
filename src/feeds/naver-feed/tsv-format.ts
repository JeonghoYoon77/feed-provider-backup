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

  pcLink({ id, cafe24PCAddress }) {
  	return `${cafe24PCAddress}${id}`
  }

  mobileLink({ id, cafe24MobileAddress }) {
  	return `${cafe24MobileAddress}${id}`
  }
}

export default TSVFormat
