class Constants {
  private _limit: number = 99999
  private _cafe24PCAddress: string = 'https://fetching.co.kr/product/detail.html?product_no='
  private _cafe24MobileAddress: string = 'https://m.fetching.co.kr/product/detail.html?product_no='
  private _condition: string = '신상품'
  private _shipping: number = 0
  private _includesVat: string = 'Y'
  private _eventWords: string = '100% 정품, 관부가세 포함, 기한한정 세일!'

  public limit(): number {
  	return this._limit
  }

  public cafe24PCAddress(): string {
  	return this._cafe24PCAddress
  }

  public cafe24MobileAddress(): string {
  	return this._cafe24MobileAddress
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
}

export default Constants
