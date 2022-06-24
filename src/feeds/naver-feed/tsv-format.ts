import { isEmpty } from 'lodash'

class TSVFormat {
  private readonly _gender: string
	private readonly _id: number | string
	private readonly _productNo: number | string
	private readonly _shopId: number
	private readonly _isCafe24Active: boolean


	constructor({ itemGender, id, shopId, productNo }) {
  	this._gender = itemGender === 'W' ? '여성' : '남성'
		this._id = id
		this._shopId = shopId
		this._productNo = productNo
	}

	public async title({ shopId, itemCode, mainName, brandName, brandNameKor, lastCategory, itemName, customColor, mpn = '',  }): Promise<string> {
		if (itemName.search(/[ㄱ-ㅎㅏ-ㅣ가-힣]/) === -1) itemName = lastCategory
		itemName = itemName.trim()

		if (itemName.includes(brandName)) itemName = itemName.replace(brandName, '').trim()
		if (itemName.includes(brandNameKor)) itemName = itemName.replace(brandNameKor, '').trim()

		let title = `${mainName} ${itemName} ${mpn ? mpn : [72, 78, 80].includes(shopId) ? '' : itemCode} ${this.color(customColor)} [관부가세 포함]`
			.split(' ').filter(str => str).join(' ')

		title = title.replace('è', 'e')
		title = title.replace('É', 'E')
		title = title.split('\n').join('')

		return title.replace(/([&"'_])/g, '').split(' ').filter(data => data).join(' ')
	}

	public link({ address }) {
		const url = new URL(`${address}${this._id}`)
		/*url.searchParams.set('utm_source', 'naver')
		url.searchParams.set('utm_medium', 'cpc')
		url.searchParams.set('utm_campaign', 'nfeed')*/
		return url.href
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
		return tags.join('|').split(' ').join('').split('\n').join(' ')
	}

	price(price: number): number {
		let rawPrice = price

		rawPrice *= 0.95

		return Math.ceil(rawPrice / 100) * 100
	}

	partnerCouponDownload(price: any) {
		return 'Y' // 쿠폰 없음
	}

	coupon(price: any) {
		return '5%'
	}
}

export default TSVFormat
