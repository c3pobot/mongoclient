'use strict'
const log = require('./logger')
let logLevel = process.env.LOG_LEVEL || log.Level.INFO;
log.setLevel(logLevel);

let mongoReady = false
const { MongoClient } = require("mongodb");
let dbo, mongo, connectionString = 'mongodb://'+process.env.MONGO_USER+':'+process.env.MONGO_PASS+'@'+process.env.MONGO_HOST+'/?compressors=zlib&retryReads=true&retryWrites=true&maxPoolSize=200'
if(process.env.MONGO_AUTH_DB) connectionString += '&authSource='+process.env.MONGO_AUTH_DB
if(process.env.MONGO_REPSET) connectionString += '&replicaSet='+process.env.MONGO_REPSET
const Cmds = {}
const mongoInit = async()=>{
  try{
    mongo = await MongoClient.connect(connectionString)
    dbo = await mongo.db(process.env.MONGO_DB)
    return true
  }catch(e){
    throw(e);
  }
}
const StartMongo = async()=>{
  try{
    let status = await mongoInit()
    if(status){
      log.info(`Mongo connection successful...`)
      mongoReady = true
      return
    }
    setTimeout(StartMongo, 5000)
  }catch(e){
    log.error(e)
    setTimeout(StartMongo, 5000)
  }
}
StartMongo()
Cmds.init = mongoInit
Cmds.aggregate = async( collection, matchCondition, data = [] )=>{
  try{
    if(matchCondition) data.unshift({$match: matchCondition})
    return await dbo.collection(collection).aggregate(data, { allowDiskUse: true }).toArray()
  }catch(e){
    throw(e);
  }
}
Cmds.del = async( collection, matchCondition )=>{
  try{
    return await dbo.collection(collection).deleteOne(matchCondition)
  }catch(e){
    throw(e)
  }
}
Cmds.delMany = async(collection, matchCondition)=>{
  try{
    return await dbo.collection(collection).deleteMany(matchCondition)
  }catch(e){
    throw(e)
  }
}
Cmds.count = async(collection, matchCondition)=>{
  try{
    return await dbo.collection( collection ).countDocuments(matchCondition)
  }catch(e){
    throw(e)
  }
}
Cmds.createIndex = async(collection, indexObj, opts = {})=>{
  try{
    if(!indexObj) throw('No index provided...')
    //opts = { background: true, expireAfterSeconds: 600 }

    return await dbo.collection( collection ).createIndex(indexObj, { ...opts, ...{ background: true }})
  }catch(e){
    throw(e)
  }
}
Cmds.listIndexes = async(collection)=>{
  try{
    return await dbo.collection( collection ).listIndexes().toArray()
  }catch(e){
    throw(e)
  }
}
Cmds.find = async(collection, matchCondition, data)=>{
  try{
    return await dbo.collection( collection ).find( matchCondition, {projection: data} ).toArray()
  }catch(e){
    throw(e)
    return []
  }
}
Cmds.limit = async(collection, matchCondition, data, limitCount = 50 )=>{
  try{
    return await dbo.collection( collection ).find( matchCondition, { projection: data } ).limit( limitCount ).toArray()
  }catch(e){
    throw(e)
    return []
  }
}
Cmds.insert = async(collection, data)=>{
  try{
    data.TTL = new Date()
    return await dbo.collection(collection).insertOne(data)
  }catch(e){
    throw (e)
  }
}
Cmds.math = async(collection, matchCondition, data)=>{
  try{
    return await dbo.collection(collection).updateOne(matchCondition, {$inc: data, $set: {TTL: new Date()}}, {"upsert":true})
  }catch(e){
    throw (e)
  }
}
Cmds.next = async(collection, matchCondition, data)=>{
  try{
    const checkCounter = await dbo.collection(collection).findOneAndUpdate(matchCondition,{ $inc:{ [data]:1 } }, {returnNewDocument:true, upsert: true} )

    if(checkCounter && checkCounter[data]) return checkCounter[data]
    const nextValue = await dbo.collection(collection).findOneAndUpdate(matchCondition,{ $inc: { [data]:1 } }, { returnNewDocument:true } )
    if(nextValue) return nextValue[data]
  }catch(e){
    throw (e)
  }
}
Cmds.push = async(collection, matchCondition, data)=>{
  try{
    return await dbo.collection(collection).updateOne(matchCondition, {$push: data, $set: {TTL: new Date()}}, {"upsert":true})
  }catch(e){
    throw (e)
  }
}
Cmds.pull = async(collection, matchCondition, data)=>{
  try{
    return await dbo.collection(collection).updateOne(matchCondition, {$pull: data, $set: {TTL: new Date()}})
  }catch(e){
    throw (e)
  }
}
Cmds.rep = async(collection, matchCondition, data)=>{
  try {
    //if(!data) return
    data.TTL = new Date()
    let res = await dbo.collection(collection).replaceOne(matchCondition, data, { upsert: true });
    delete data.TTL
    return res
  } catch (e) {
    throw (e)
  }
}
Cmds.set = async( collection, matchCondition, data )=>{
  try{
    if(!data.TTL) data.TTL = new Date()
    let res = await dbo.collection(collection).updateOne(matchCondition,{$set: data },{"upsert":true})
    delete data.TTL
    return res
  }catch(e){
    throw (e)
  }
}
Cmds.setMany = async(collection, matchCondition, data )=>{
  try{
    if(!data.TTL) data.TTL = new Date()
    let res = await dbo.collection(collection).updateMany(matchCondition, {$set: data}, {upsert: true})
    delete data.TTL
    return res
  }catch(e){
    throw(e)
  }
}
Cmds.skip = async(collection, matchCondition, data, limitCount = 50, skipCount = 50)=>{
  try{
    return await dbo.collection( collection ).find( matchCondition, { projection: data } ).limit( limitCount ).skip( skipCount ).toArray()
  }catch(e){
    throw(e)
    return []
  }
}
Cmds.status = () =>{
  return mongoReady
}
Cmds.unset = async(collection, matchCondition, data)=>{
  try{
    return await dbo.collection(collection).updateOne(matchCondition, {$unset: data, $set: {TTL: new Date()}})
  }catch(e){
    throw (e)
  }
}
module.exports = Cmds
