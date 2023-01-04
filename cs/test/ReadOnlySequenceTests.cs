using FASTER.core;
using Microsoft.IO;
using NUnit.Framework;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using static FASTER.test.ReadCacheTests.RandomReadCacheTests;
using static FASTER.test.ReadOnlySequenceTests;

namespace FASTER.test
{
    [TestFixture]
    internal class ReadOnlySequenceTests
    {
        static readonly RecyclableMemoryStreamManager recyclableMemoryStreamManager = new ();

        private static readonly byte[] Data = Enumerable.Range(0, 1024).Select(d => (byte)d).ToArray();

        private static readonly byte[] BigData = Enumerable.Range(0, 1_073_741_824).Select(d => (byte)d).ToArray();

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public unsafe void RecyclableMemoryStreamTest1()
        {
            Span<byte> output = stackalloc byte[20];
            ReadOnlySequence<byte> input = default;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            try
            {
                using var log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/hlog1.log", deleteOnClose: true);

                // With non-blittable types, you need an object log device in addition to the
                // main device. FASTER serializes the actual objects in the object log.
                var objlog = Devices.CreateLogDevice(TestUtils.MethodTestDir + "hlog.obj.log");

                // Serializers are required for class types in order to write to storage
                // You can also mark types as DataContract (lower performance)
                var serializerSettings =
                    new SerializerSettings<SpanByte, ReadOnlySequence<byte>>
                    {
                        valueSerializer = () => new CacheValueSerializer()
                    };

                using var fht = new FasterKV<SpanByte, ReadOnlySequence<byte>>
                    (128, new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MemorySizeBits = 17, PageSizeBits = 12 }
                    , serializerSettings: serializerSettings
                    );
                using var s = fht.NewSession(new RecyclableMemoryStreamFunctions<SpanByte, Empty>());

                var key1 = SpanByte.FromFixedSpan(MemoryMarshal.Cast<char, byte>("key1".AsSpan()));
                var value1 = MemoryMarshal.Cast<char, byte>("value1".AsSpan());
                var output1 = (recyclableMemoryStreamManager.GetStream(output) as RecyclableMemoryStream).GetReadOnlySequence(); //SpanByteAndMemory.FromFixedSpan(output);

                var valueStream1 = recyclableMemoryStreamManager.GetStream(value1) as RecyclableMemoryStream;

                var value1Buf = valueStream1.GetReadOnlySequence();
                s.Upsert(ref key1, ref value1Buf);
                s.Read(ref key1, ref input, ref output1);

                //var outBuf = output1.GetBuffer();
                Assert.IsTrue(output1.ToArray().AsSpan().Slice(0, (int)output1.Length).SequenceEqual(value1));

                var key2 = SpanByte.FromFixedSpan(MemoryMarshal.Cast<char, byte>("key2".AsSpan()));
                var value2 = MemoryMarshal.Cast<char, byte>("value2value2value2".AsSpan());
                var output2 = (recyclableMemoryStreamManager.GetStream(output) as RecyclableMemoryStream).GetReadOnlySequence(); //SpanByteAndMemory.FromFixedSpan(output);

                var valueStream2 = recyclableMemoryStreamManager.GetStream(value2) as RecyclableMemoryStream;
                var value2Buf = valueStream2.GetReadOnlySequence();
                s.Upsert(key2, value2Buf);
                s.Read(ref key2, ref input, ref output2);

                Assert.IsTrue(output2.ToArray().AsSpan().Slice(0, (int)output2.Length).SequenceEqual(value2));
            }
            finally
            {
                TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            }
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public unsafe void MultiReadSpanByteKeyTest()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            try
            {
                using var log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/MultiReadSpanByteKeyTest.log", deleteOnClose: true);
                // With non-blittable types, you need an object log device in addition to the
                // main device. FASTER serializes the actual objects in the object log.
                var objlog = Devices.CreateLogDevice(TestUtils.MethodTestDir + "MultiReadSpanByteKeyTest.obj.log");

                // Serializers are required for class types in order to write to storage
                // You can also mark types as DataContract (lower performance)
                var serializerSettings =
                    new SerializerSettings<SpanByte, ReadOnlySequence<byte>>
                    {
                        valueSerializer = () => new CacheValueSerializer()
                    };

                using var fht = new FasterKV<SpanByte, ReadOnlySequence<byte>>(
                    size: 1L << 10,
                    new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MemorySizeBits = 15, PageSizeBits = 12 }
                    , serializerSettings: serializerSettings);
                using var session = fht.For(new RecyclableMemoryStreamFunctions<SpanByte, Empty>()).NewSession<RecyclableMemoryStreamFunctions<SpanByte, Empty>>();

                for (int i = 0; i < 200; i++)
                {
                    using var valueStream = recyclableMemoryStreamManager.GetStream() as RecyclableMemoryStream;
                    using var writer = new BinaryWriter(valueStream, Encoding.UTF8, leaveOpen: true);
                    writer.Write(i);

                    var buf = valueStream.GetReadOnlySequence();

                    var key = MemoryMarshal.Cast<char, byte>($"{i}".AsSpan());
                    fixed (byte* _ = key)
                        session.Upsert(SpanByte.FromFixedSpan(key), buf);
                }

                // Evict all records to disk
                fht.Log.FlushAndEvict(true);


                for (long key = 0; key < 50; key++)
                {
                    // read each key multiple times
                    for (int i = 0; i < 10; i++)
                        Assert.AreEqual(key, ReadKey($"{key}"));
                }

                long ReadKey(string keyString)
                {
                    Status status;

                    var key = MemoryMarshal.Cast<char, byte>(keyString.AsSpan());
                    fixed (byte* _ = key)
                        status = session.Read(key: SpanByte.FromFixedSpan(key), out var unused);

                    // All keys need to be fetched from disk
                    Assert.IsTrue(status.IsPending);

                    session.CompletePendingWithOutputs(out var completedOutputs, wait: true);

                    var count = 0;
                    ReadOnlySequence<byte> value = default;
                    using (completedOutputs)
                    {
                        while (completedOutputs.Next())
                        {
                            count++;
                            Assert.IsTrue(completedOutputs.Current.Status.Found);
                            value = completedOutputs.Current.Output;
                        }
                    }
                    Assert.AreEqual(1, count);

                    using var valueStream = recyclableMemoryStreamManager.GetStream() as RecyclableMemoryStream;
                    foreach(var item in value)
                    {
                        valueStream.Write(item.Span);
                    }
                    valueStream.Position = 0;
                    using var reader = new BinaryReader(valueStream);
                    var longValue = reader.ReadInt64();

                    return longValue;
                }
            }
            finally
            {
                TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            }
        }


        [Test]
        public void CacheValueSerializerTest()
        {
            var src = recyclableMemoryStreamManager.GetStream() as RecyclableMemoryStream;
            try
            {
                src.Write(Data);

                using var streamSerializer = recyclableMemoryStreamManager.GetStream() as RecyclableMemoryStream;
                var serializer = new CacheValueSerializer();
                serializer.BeginSerialize(streamSerializer);
                var srcBuf = src.GetReadOnlySequence();
                serializer.Serialize(ref srcBuf);

                using var streamDeserialize = recyclableMemoryStreamManager.GetStream() as RecyclableMemoryStream;
                src.WriteTo(streamDeserialize);

                streamDeserialize.Position = 0;
                serializer.BeginDeserialize(streamDeserialize);
                serializer.Deserialize(out var dest);

                Assert.AreEqual(srcBuf.ToArray(), dest.ToArray());
            }
            finally
            {
                src.Dispose();
            }
        }


        [Test]
        public void CacheValueSerializerBigDataTest()
        {
            var src = recyclableMemoryStreamManager.GetStream() as RecyclableMemoryStream;
            try
            {
                src.Write(BigData);

                using var streamSerializer = recyclableMemoryStreamManager.GetStream() as RecyclableMemoryStream;
                var serializer = new CacheValueSerializer();
                serializer.BeginSerialize(streamSerializer);
                var srcBuf = src.GetReadOnlySequence();
                serializer.Serialize(ref srcBuf);


                using var streamDeserialize = recyclableMemoryStreamManager.GetStream() as RecyclableMemoryStream;
                src.WriteTo(streamDeserialize);

                streamDeserialize.Position = 0;
                serializer.BeginDeserialize(streamDeserialize);
                serializer.Deserialize(out var dest);

                Assert.True(srcBuf.ToArray().AsSpan().SequenceEqual( dest.ToArray()));
            }
            finally
            {
                src.Dispose();
            }

        }

        public class RecyclableMemoryStreamFunctions<Key, Context> : SimpleFunctions<Key, ReadOnlySequence<byte>, Context>
        {

        }


        /// <summary>
        /// Serializer for CacheValue - used if CacheValue is changed from struct to class
        /// </summary>
        public class CacheValueSerializer : BinaryObjectSerializer<ReadOnlySequence<byte>>
        {
            private sealed class BlockSegment : ReadOnlySequenceSegment<byte>
            {
                public BlockSegment(Memory<byte> memory) => Memory = memory;

                public BlockSegment Append(Memory<byte> memory)
                {
                    var nextSegment = new BlockSegment(memory) { RunningIndex = RunningIndex + Memory.Length };
                    Next = nextSegment;
                    return nextSegment;
                }
            }


            public override void Deserialize(out ReadOnlySequence<byte> obj)
            {
                using var stream = recyclableMemoryStreamManager.GetStream() as RecyclableMemoryStream;

                while (true)
                {
                    var span = stream.GetSpan();
                    var readLen = reader.Read(span);
                    if (readLen <= 0)
                    {
                        break;
                    }
                    stream.Advance(readLen);
                }

                obj = stream.GetReadOnlySequence();
            }

            public override void Serialize(ref ReadOnlySequence<byte> obj)
            {
                writer.Write(obj.Length);
                foreach (var item in obj)
                {
                    writer.Write(item.Span);
                }
            }
        }
    }
}
