class Constants {
  private _limit: number = 100000
  private _cafe24PCAddress: string = 'https://fetching.co.kr/product/detail.html?product_no='
  private _cafe24MobileAddress: string = 'https://m.fetching.co.kr/product/detail.html?product_no='
  private _cafe24PCAddressApp: string = 'https://fetching.co.kr/product_detail_app.html?product_no='
  private _cafe24MobileAddressApp: string = 'https://m.fetching.co.kr/product_detail_app.html?product_no='
  private _condition: string = '신상품'
  private _shipping: number = 0
  private _includesVat: string = 'Y'
  private _eventWords: string = '200% 정품 보상, 관부가세 포함, 5% 할인 쿠폰'
  private _eventWordsApp: string = '200% 정품 보상, 관부가세 포함, 5% 할인 쿠폰'

  public limit(): number {
  	return this._limit
  }

  public cafe24PCAddress(): string {
  	return this._cafe24PCAddress
  }

  public cafe24PCAddressApp(): string {
  	return this._cafe24PCAddressApp
  }

  public cafe24MobileAddress(): string {
  	return this._cafe24MobileAddress
  }

  public cafe24MobileAddressApp(): string {
  	return this._cafe24MobileAddressApp
  }

  public condition(): string {
  	return this._condition
  }

  public shipping(): number {
  	return this._shipping
  }

  public includesVat(): string {
  	return this._includesVat
  }

  public eventWords(): string {
  	return this._eventWords
  }

  public eventWordsApp(): string {
  	return this._eventWordsApp
  }
}

export default Constants
