// @flow

import {
  // $FlowFixMe
  promiseAllObject,
  map,
  reduce,
  values,
  pipe,
  equals,
} from 'rambdax'
import { unnest } from '../../utils/fp'
import { logError } from '../../utils/common'
import type { Database, Model, TableName } from '../..'

import { prepareMarkAsSynced, ensureActionsEnabled } from './helpers'
import type { SyncLocalChanges } from './fetchLocal'
import type { SyncPushResult } from '..'
import { sanitizedRaw } from '../../RawRecord'

const unchangedRecordsForRaws = (raws, recordCache) =>
  reduce(
    (records, raw) => {
      const record = recordCache.find(model => model.id === raw.id)
      if (!record) {
        logError(
          `[Sync] Looking for record ${
            raw.id
          } to mark it as synced, but I can't find it. Will ignore it (it should get synced next time). This is probably a Watermelon bug — please file an issue!`,
        )
        return records
      }

      // only include if it didn't change since fetch
      // TODO: get rid of `equals`
      return equals(record._raw, raw) ? records.concat(record) : records
    },
    [],
    raws,
  )

const recordsToMarkAsSynced = ({ changes, affectedRecords }: SyncLocalChanges): Model[] =>
  pipe(
    values,
    map(({ created, updated }) =>
      unchangedRecordsForRaws([...created, ...updated], affectedRecords),
    ),
    unnest,
  )(changes)

const destroyDeletedRecords = (db: Database, { changes }: SyncLocalChanges): Promise<*> =>
  promiseAllObject(
    map(
      ({ deleted }, tableName) => db.adapter.destroyDeletedRecords(tableName, deleted),
      // $FlowFixMe
      changes,
    ),
  )

export default function markLocalChangesAsSynced(
  db: Database,
  syncedLocalChanges: SyncLocalChanges,
  pushResults: ?SyncPushResult
): Promise<void> {
  ensureActionsEnabled(db)
  return db.action(async () => {
    // update and destroy records concurrently
    await Promise.all([
      db.batch(...map(prepareMarkAsSynced, recordsToMarkAsSynced(syncedLocalChanges))),
      destroyDeletedRecords(db, syncedLocalChanges),
    ])

    if (pushResults && pushResults.updatedCreateTables) {
      for (const tableKey in pushResults.updatedCreateTables) {
        if (tableKey) {
          // $FlowFixMe
          const table = pushResults.updatedCreateTables[tableKey]
          for (const id in table) {
            // $FlowFixMe
            const collection = db.collections.get(tableKey)
            const model = await collection.find(id)
            if (model) {
              await collection._destroyPermanently(model)
              await collection.create(record => {
                const raw = {}
                for (const key in model._raw) {
                  if (key && key !== 'id' && key[0] !== '_') {
                    raw[key] = model._raw[key]
                  }
                }
                for (const key in model) {
                  if (key && key !== 'id' && key !== 'collection' && key !== 'syncStatus' && key[0] !== '_') {
                    record[key] = model[key]
                  }
                }
                raw.id = table[id]
                raw._status = 'synced'
                record._raw = sanitizedRaw(raw, collection.schema)
              })
            }
          }
        }
      }
    }

  }, 'sync-markLocalChangesAsSynced')
}
