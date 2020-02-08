import monk, { ICollection, IMonkManager } from "monk";
import { ObjectId } from "bson";
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
  _id: ObjectId;
  [key: string]: any;
};

export type Index = {
  [key: string]: 1 | -1 | "text";
};

export type OptionalId<TSchema> = Omit<TSchema, "_id"> & {
  _id?: Document["_id"];
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

export function id(str: null): null;

export function id(str: string): ObjectId;

export function id(str: string | null): ObjectId | null {
  return str && ObjectId.isValid(str)
    ? ObjectId.createFromHexString(str)
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
    query: FilterQuery<TSchema>,
    options?: MongoCountPreferences
  ): Promise<number> {
    return this.collection.count(query, options);
  }

  exists(query: FilterQuery<TSchema>): Promise<boolean> {
    return this.collection.count(query, { limit: 1 }).then(count => !!count);
  }

  existsById(id: string | ObjectId): Promise<boolean> {
    return this.collection
      .count({ _id: id }, { limit: 1 })
      .then(count => !!count);
  }

  insert(document: OptionalId<TSchema>): Promise<TSchema> {
    return this.collection.insert(document);
  }

  find(
    query: FilterQuery<TSchema>,
    options?: FindOneOptions | string
  ): Promise<TSchema[]> {
    return this.collection.find(query, options);
  }

  async findOne(
    query: FilterQuery<TSchema>,
    options?: FindOneOptions | string
  ): Promise<TSchema | null> {
    const doc = await this.collection.findOne(query, options);
    return doc || null;
  }

  async findById(
    id: string | ObjectId,
    options?: FindOneOptions | string
  ): Promise<TSchema | null> {
    const doc = await this.collection.findOne({ _id: id }, options);
    return doc || null;
  }

  async findLastOne(query: FilterQuery<TSchema>): Promise<TSchema | null> {
    const doc = await this.collection.findOne(query, {
      sort: { _id: -1 },
      limit: 1
    });
    return doc || null;
  }

  findAndMap<Key extends string>(
    query: FilterQuery<TSchema>,
    key: Key
  ): Promise<TSchema[Key][]> {
    return this.collection
      .find(query, key)
      .then(list => list.map(document => document[key]));
  }

  async findSmartPage(
    query: FilterQuery<TSchema>,
    search: string | null,
    sort: string | null,
    sortOrder: number | null,
    limit: number,
    page: number
  ): Promise<PageResult<TSchema>> {
    const rootQuery = {
      $and: [query, ...(search ? [{ $text: { $search: search } }] : [])]
    };
    return {
      total: await this.collection.count(rootQuery),
      items: await this.collection.find(rootQuery, {
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

  async findOneAndUpdate(
    filter: FilterQuery<TSchema>,
    update: UpdateQuery<TSchema> | TSchema,
    options?: FindOneAndUpdateOption | string
  ): Promise<TSchema | null> {
    const doc = await this.collection.findOneAndUpdate(filter, update, options);
    return doc || null;
  }

  async findByIdAndSet(
    id: string | ObjectId,
    $set: UpdateQuery<TSchema>["$set"],
    options?: FindOneAndUpdateOption | string
  ): Promise<TSchema | null> {
    const doc = await this.collection.findOneAndUpdate(
      { _id: id },
      { $set },
      options
    );
    return doc || null;
  }

  remove(
    filter: FilterQuery<TSchema>,
    options?: CommonOptions & { multi?: boolean }
  ): Promise<RemoveResult> {
    return this.collection.remove(filter, options);
  }

  async findOneAndDelete(
    filter: FilterQuery<TSchema>,
    options?: FindOneOptions | string
  ): Promise<TSchema | null> {
    const doc = await this.collection.findOneAndDelete(filter, options);
    return doc || null;
  }

  async findByIdAndDelete(
    id: string | ObjectId,
    options?: FindOneOptions | string
  ): Promise<TSchema | null> {
    const doc = await this.collection.findOneAndDelete({ _id: id }, options);
    return doc || null;
  }
}
