import monk, { ICollection, IMonkManager } from "monk";
import { ObjectID } from "bson";
import {
  CommonOptions,
  FilterQuery,
  FindOneAndUpdateOption,
  FindOneOptions,
  MongoCountPreferences,
  UpdateOneOptions,
  UpdateQuery
} from "mongodb";

export type Document = {
  _id: ObjectID;
  [key: string]: any;
};

export type Index = {
  [key: string]: 1 | -1 | "text";
};

export type OptionalId<TSchema extends Document> = Omit<TSchema, "_id"> & {
  _id?: TSchema["_id"];
};

export type PageResult<TSchema> = {
  total: number;
  items: TSchema[];
};

export type UpdateResult = {
  ok: 1 | 0;
  nModified: number;
  n: number;
};

export type RemoveResult = {
  deletedCount: number;
  result: {
    n: number;
    ok: 1 | 0;
  };
};

export { ObjectID };

export function idify(str: null | undefined): null;

export function idify(str: string): ObjectID;

export function idify(str: string | null | undefined): ObjectID | null;

export function idify(str: string | null | undefined): ObjectID | null {
  return str && ObjectID.isValid(str)
    ? ObjectID.createFromHexString(str)
    : null;
}

export class Manager {
  manager: IMonkManager;

  constructor(uri: string) {
    this.manager = monk(uri);
  }

  get<TSchema extends Document>(name: string) {
    return new Collection<TSchema>(this.manager.get<TSchema>(name));
  }
}

export class Collection<TSchema extends Document> {
  collection: ICollection<TSchema>;

  constructor(col: ICollection<TSchema>) {
    this.collection = col;
  }

  createIndex(index: Index): Promise<string> {
    return this.collection.createIndex(index as any);
  }

  count(
    filter: FilterQuery<TSchema>,
    options?: MongoCountPreferences
  ): Promise<number> {
    return this.collection.count(filter, options);
  }

  exists(filter: FilterQuery<TSchema>): Promise<boolean> {
    return this.collection.count(filter, { limit: 1 }).then(count => !!count);
  }

  existsById(id: string | ObjectID): Promise<boolean> {
    return this.collection
      .count({ _id: id }, { limit: 1 })
      .then(count => !!count);
  }

  insert(document: OptionalId<TSchema>): Promise<TSchema> {
    return this.collection.insert(document);
  }

  find(
    filter: FilterQuery<TSchema>,
    options?: FindOneOptions | string
  ): Promise<TSchema[]> {
    return this.collection.find(filter, options);
  }

  findOne(
    filter: FilterQuery<TSchema>,
    options?: FindOneOptions | string
  ): Promise<TSchema | null> {
    return this.collection.findOne(filter, options).then(doc => doc || null);
  }

  findById(
    id: string | ObjectID,
    options?: FindOneOptions | string
  ): Promise<TSchema | null> {
    return this.collection
      .findOne({ _id: id }, options)
      .then(doc => doc || null);
  }

  findLastOne(filter: FilterQuery<TSchema>): Promise<TSchema | null> {
    return this.collection
      .findOne(filter, {
        sort: { _id: -1 },
        limit: 1
      })
      .then(doc => doc || null);
  }

  findAndMap<Key extends string>(
    filter: FilterQuery<TSchema>,
    key: Key
  ): Promise<TSchema[Key][]> {
    return this.collection
      .find(filter, key)
      .then(list => list.map(document => document[key]));
  }

  async findSmartPage(
    filter: FilterQuery<TSchema>,
    search: string | null,
    sort: string | null,
    sortOrder: number | null,
    limit: number,
    page: number
  ): Promise<PageResult<TSchema>> {
    const rootFilter = {
      $and: [filter, ...(search ? [{ $text: { $search: search } }] : [])]
    };
    return {
      total: await this.collection.count(rootFilter),
      items: await this.collection.find(rootFilter, {
        ...(sort && sortOrder && { sort: { [sort]: sortOrder } }),
        skip: (page - 1) * limit,
        limit
      })
    };
  }

  update(
    filter: FilterQuery<TSchema>,
    update: UpdateQuery<TSchema> | Partial<TSchema>,
    options?: UpdateOneOptions & { multi?: boolean }
  ): Promise<UpdateResult> {
    return this.collection.update(filter, update, options);
  }

  findOneAndUpdate(
    filter: FilterQuery<TSchema>,
    update: UpdateQuery<TSchema> | TSchema,
    options?: FindOneAndUpdateOption | string
  ): Promise<TSchema | null> {
    return this.collection
      .findOneAndUpdate(filter, update, options)
      .then(doc => doc || null);
  }

  findByIdAndSet(
    id: string | ObjectID,
    $set: UpdateQuery<TSchema>["$set"],
    options?: FindOneAndUpdateOption | string
  ): Promise<TSchema | null> {
    return this.collection
      .findOneAndUpdate({ _id: id }, { $set }, options)
      .then(doc => doc || null);
  }

  remove(
    filter: FilterQuery<TSchema>,
    options?: CommonOptions & { multi?: boolean }
  ): Promise<RemoveResult> {
    return this.collection.remove(filter, options);
  }

  findOneAndDelete(
    filter: FilterQuery<TSchema>,
    options?: FindOneOptions | string
  ): Promise<TSchema | null> {
    return this.collection
      .findOneAndDelete(filter, options)
      .then(doc => doc || null);
  }

  findByIdAndDelete(
    id: string | ObjectID,
    options?: FindOneOptions | string
  ): Promise<TSchema | null> {
    return this.collection
      .findOneAndDelete({ _id: id }, options)
      .then(doc => doc || null);
  }
}

export default (uri: string) => new Manager(uri);
