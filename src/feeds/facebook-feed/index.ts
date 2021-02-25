import { RowDataPacket } from 'mysql2/promise'

import { MySQL } from '../../utils'
import Form, { FormProps } from './form'
import Query from './query'
import Upload from './upload'

async function FacebookFeed() {
  const limit: number = 99999
  const rows: RowDataPacket[] = await <Promise<RowDataPacket[]>>MySQL.execute(Query(limit))

  const contents: FormProps[] = Form(rows)
  await Upload(contents)
}

export default FacebookFeed
