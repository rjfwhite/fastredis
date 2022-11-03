namespace Core.Redis
{
   interface IRedisClient
   {
       public long SendCommand(params string[] command);

       public long SendCommand(byte[][] command);

       public bool TryReceiveResult(out RedisValue result, out long handle);
   }
}