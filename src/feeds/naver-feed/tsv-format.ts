import { isEmpty } from 'lodash'

class TSVFormat {
	private readonly _gender: string
	private readonly _id: number | string
	private readonly _productNo: number | string
	private readonly _shopId: number
	private readonly _isCafe24Active: boolean


	constructor({ itemGender, id, shopId, productNo }) {
		this._gender = itemGender === 'W' ? '여성' : itemGender === 'M' ? '남성' : ''
		this._id = id
		this._shopId = shopId
		this._productNo = productNo
	}

	public async title({ idx, shopId, itemCode, mainName, brandName, brandNameKor, lastCategory, itemName, customColor, mpn = '', season = '' }): Promise<string> {
		// 수정할 시, 상품 목록에 있는 네이버 피드 이름도 같이 수정할 것
		if (itemName.search(/[ㄱ-ㅎㅏ-ㅣ가-힣]/) === -1) itemName = lastCategory
		itemName = itemName.trim()

		if (itemName.includes(brandName)) itemName = itemName.replace(brandName, '').trim()
		if (itemName.includes(brandNameKor)) itemName = itemName.replace(brandNameKor, '').trim()

		if (shopId === 16) itemCode = itemCode.slice(0, 9)

		const code = `${mpn ? mpn : ''}${' ' + ([72, 78, 80].includes(shopId) ? idx : itemCode)}`

		let title = `${mainName} ${itemName} ${this.color(customColor)} ${code.replace(/([^\dA-z ])/g, ' ')} ${season || ''} ${this._gender}`
			.split(' ').filter(str => str).join(' ')

		title = title.replace('è', 'e')
		title = title.replace('É', 'E')
		title = title.split('\n').join('')

		return title.replace(/([&"'_])/g, ' ').split(' ').filter(data => data).join(' ')
	}

	public link({ address }) {
		const url = new URL(`${address}${this._id}`)
		url.searchParams.set('utm_source', 'naver')
		url.searchParams.set('utm_medium', 'cps')
		url.searchParams.set('utm_campaign', 'nfeed')
		url.searchParams.set('PARTNERID', 'naver_ep_pc')
		return url.href
	}

	public mobileLink({ address }) {
		const url = new URL(`${address}${this._id}`)
		url.searchParams.set('utm_source', 'naver')
		url.searchParams.set('utm_medium', 'cps')
		url.searchParams.set('utm_campaign', 'nfeed')
		url.searchParams.set('PARTNERID', 'naver_ep_mo')
		return url.href
	}

	public gender() {
		return this._gender
	}

	public color(customColor: string) {
		return isEmpty(customColor)
			? ''
			: ' ' + customColor
				.replace(/[^a-zA-Zㄱ-ㅎㅏ-ㅣ가-힣0-9]/gi, ' ')
				.toUpperCase()
				.trim()
	}

	public searchTag({brandName, brandNameKor, categoryName2, categoryName3, color, designerStyleId, originName, itemName, brandSemiName, categorySemiName}) {
		itemName = itemName.trim()
		const tags = [
			brandName, brandNameKor, categoryName2, categoryName3, color, designerStyleId, originName, itemName, brandSemiName, categorySemiName
		]
		return tags.flat().map(str => str?.replace(/\\t/g, '')?.trim()).filter(str => str).join('|').split('\n').join('').split(' ').join('')
	}

	price(price: number): number {
		let rawPrice = price

		rawPrice *= 0.94

		return Math.ceil(rawPrice / 100) * 100
	}

	priceMobile(price: number): number {
		let rawPrice = price

		rawPrice *= 0.94

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
