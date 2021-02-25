import { RowDataPacket } from 'mysql2/promise'

import { MySQL } from '../../utils'
import Format, { Form } from './form'
import Query from './query'
import Upload from './upload'

async function FacebookFeed() {
  const limit: number = 99999

  const rows: RowDataPacket[] = await <Promise<RowDataPacket[]>>MySQL.execute(Query(limit))
  console.log(`finish query: ${rows.length} lines`)

  const contents: Form[] = Format(rows)
  console.log('finish format')
  await Upload(contents)
  console.log('finish upload to google spread sheet')
}

export default FacebookFeed
