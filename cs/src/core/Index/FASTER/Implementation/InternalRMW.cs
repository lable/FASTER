﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        internal bool ReinitializeExpiredRecord<Input, Output, Context, FasterSession>(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo,
                                                                                       long logicalAddress, FasterExecutionContext<Input, Output, Context> sessionCtx, FasterSession fasterSession,
                                                                                       bool isIpu, out OperationStatus status)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            // This is called for InPlaceUpdater or CopyUpdater only; CopyUpdater however does not copy an expired record, so we return CreatedRecord.
            var advancedStatusCode = isIpu ? StatusCode.InPlaceUpdatedRecord : StatusCode.CreatedRecord;
            advancedStatusCode |= StatusCode.Expired;
            if (!fasterSession.NeedInitialUpdate(ref key, ref input, ref output, ref rmwInfo))
            {
                if (rmwInfo.Action == RMWAction.CancelOperation)
                {
                    status = OperationStatus.CANCELED;
                    return false;
                }
                else
                {
                    // Expiration with no insertion.
                    recordInfo.Tombstone = true;
                    status = OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, advancedStatusCode);
                    return true;
                }
            }

            // Try to reinitialize in place
            (var currentSize, _) = hlog.GetRecordSize(ref key, ref value);
            (var requiredSize, _) = hlog.GetInitialRecordSize(ref key, ref input, fasterSession);

            if (currentSize >= requiredSize)
            {
                if (fasterSession.InitialUpdater(ref key, ref input, ref value, ref output, ref recordInfo, ref rmwInfo))
                {
                    // If IPU path, we need to complete PostInitialUpdater as well
                    if (isIpu)
                        fasterSession.PostInitialUpdater(ref key, ref input, ref value, ref output, ref recordInfo, ref rmwInfo);

                    status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, advancedStatusCode);
                    return true;
                }
                else
                {
                    if (rmwInfo.Action == RMWAction.CancelOperation)
                    {
                        status = OperationStatus.CANCELED;
                        return false;
                    }
                    else
                    {
                        // Expiration with no insertion.
                        recordInfo.Tombstone = true;
                        status = OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, advancedStatusCode);
                        return true;
                    }
                }
            }

            // Reinitialization in place was not possible. InternalRMW will do the following based on who called this:
            //  IPU: move to the NIU->allocate->IU path
            //  CU: caller invalidates allocation, retries operation as NIU->allocate->IU
            status = OperationStatus.SUCCESS;
            return false;
        }

        /// <summary>
        /// Read-Modify-Write Operation. Updates value of 'key' using 'input' and current value.
        /// Pending operations are processed either using InternalRetryPendingRMW or 
        /// InternalContinuePendingRMW.
        /// </summary>
        /// <param name="key">key of the record.</param>
        /// <param name="input">input used to update the value.</param>
        /// <param name="output">Location to store output computed from input and value.</param>
        /// <param name="userContext">user context corresponding to operation used during completion callback.</param>
        /// <param name="pendingContext">pending context created when the operation goes pending.</param>
        /// <param name="fasterSession">Callback functions.</param>
        /// <param name="sessionCtx">Session context</param>
        /// <param name="lsn">Operation serial number</param>
        /// <returns>
        /// <list type="table">
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>SUCCESS</term>
        ///     <term>The value has been successfully updated (or inserted).</term>
        ///     </item>
        ///     <item>
        ///     <term>RECORD_ON_DISK</term>
        ///     <term>The record corresponding to 'key' is on disk. Issue async IO to retrieve record and retry later.</term>
        ///     </item>
        ///     <item>
        ///     <term>RETRY_LATER</term>
        ///     <term>Cannot  be processed immediately due to system state. Add to pending list and retry later.</term>
        ///     </item>
        ///     <item>
        ///     <term>CPR_SHIFT_DETECTED</term>
        ///     <term>A shift in version has been detected. Synchronize immediately to avoid violating CPR consistency.</term>
        ///     </item>
        /// </list>
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus InternalRMW<Input, Output, Context, FasterSession>(
                                   ref Key key, ref Input input, ref Output output,
                                   ref Context userContext,
                                   ref PendingContext<Input, Output, Context> pendingContext,
                                   FasterSession fasterSession,
                                   FasterExecutionContext<Input, Output, Context> sessionCtx,
                                   long lsn)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            OperationStatus status = default;
            var latchOperation = LatchOperation.None;
            var latchDestination = LatchDestination.NormalProcessing;

            OperationStackContext<Key, Value> stackCtx = new(comparer.GetHashCode64(ref key));

            if (sessionCtx.phase != Phase.REST)
                HeavyEnter(stackCtx.hei.hash, sessionCtx, fasterSession);

            // A 'ref' variable must be initialized. If we find a record for the key, we reassign the reference.
            RecordInfo dummyRecordInfo = default;
            ref RecordInfo srcRecordInfo = ref dummyRecordInfo;

            FindOrCreateTag(ref stackCtx.hei);
            stackCtx.SetRecordSourceToHashEntry(hlog);

            // This tracks the address pointed to by the hash bucket; it may or may not be in the readcache, in-memory, on-disk, or < BeginAddress.
            // InternalContinuePendingRMW can stop comparing keys immediately above this address.
            long prevHighestKeyHashAddress = stackCtx.hei.Address;

            FindRecordInMemory(ref key, ref stackCtx, hlog.HeadAddress);

            RMWInfo rmwInfo = new()
            {
                SessionType = fasterSession.SessionType,
                Version = sessionCtx.version,
                SessionID = sessionCtx.sessionID,
                Address = stackCtx.recSrc.LogicalAddress,
                KeyHash = stackCtx.hei.hash
            };

            #region Entry latch operation if necessary
            if (sessionCtx.phase != Phase.REST)
            {
                latchDestination = AcquireLatchRMW(pendingContext, sessionCtx, ref stackCtx.hei, ref status, ref latchOperation, stackCtx.recSrc.LogicalAddress);
            }
            #endregion Entry latch operation if necessary

            // We must use try/finally to ensure unlocking even in the presence of exceptions.
            try
            {
            #region Normal processing

                if (latchDestination == LatchDestination.NormalProcessing)
                {
                    if (stackCtx.recSrc.HasReadCacheSrc)
                    {
                        // Use the readcache record as the CopyUpdater source.
                        goto LockSourceRecord;
                    }
                    else if (stackCtx.recSrc.LogicalAddress >= hlog.ReadOnlyAddress)
                    {
                        // Mutable Region: Update the record in-place
                        srcRecordInfo = ref hlog.GetInfo(stackCtx.recSrc.PhysicalAddress);
                        if (!TryEphemeralXLock<Input, Output, Context, FasterSession>(fasterSession, ref stackCtx.recSrc, ref srcRecordInfo, out status))
                            goto LatchRelease;

                        if (srcRecordInfo.Tombstone)
                            goto CreateNewRecord;

                        if (!srcRecordInfo.IsValidUpdateOrLockSource)
                        {
                            EphemeralXUnlockAndAbandonUpdate<Input, Output, Context, FasterSession>(fasterSession, ref stackCtx.recSrc, ref srcRecordInfo);
                            srcRecordInfo = ref dummyRecordInfo;
                            goto CreateNewRecord;
                        }

                        rmwInfo.RecordInfo = srcRecordInfo;
                        if (fasterSession.InPlaceUpdater(ref key, ref input, ref hlog.GetValue(stackCtx.recSrc.PhysicalAddress), ref output, ref srcRecordInfo, ref rmwInfo, out status)
                            || (rmwInfo.Action == RMWAction.ExpireAndStop))
                        {
                            this.MarkPage(stackCtx.recSrc.LogicalAddress, sessionCtx);

                            // ExpireAndStop means to override default Delete handling (which is to go to InitialUpdater) by leaving the tombstoned record as current.
                            // Our IFasterSession.InPlaceUpdater implementation has already reinitialized-in-place or set Tombstone as appropriate (inside the ephemeral lock)
                            // and marked the record.
                            pendingContext.recordInfo = srcRecordInfo;
                            pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
                            goto LatchRelease;
                        }
                        if (OperationStatusUtils.BasicOpCode(status) != OperationStatus.SUCCESS)
                            goto LatchRelease;

                        // InPlaceUpdater failed (e.g. insufficient space, another thread set Tombstone, etc). Use this record as the CopyUpdater source.
                        stackCtx.recSrc.HasMainLogSrc = true;
                        goto CreateNewRecord;
                    }
                    else if (stackCtx.recSrc.LogicalAddress >= hlog.SafeReadOnlyAddress && !hlog.GetInfo(stackCtx.recSrc.PhysicalAddress).Tombstone)
                    {
                        // Fuzzy Region: Must retry after epoch refresh, due to lost-update anomaly
                        status = OperationStatus.RETRY_LATER;
                        goto LatchRelease;
                    }
                    else if (stackCtx.recSrc.LogicalAddress >= hlog.HeadAddress)
                    {
                        // Safe Read-Only Region: CopyUpdate to create a record in the mutable region
                        stackCtx.recSrc.HasMainLogSrc = true;
                        goto LockSourceRecord;
                    }
                    else if (stackCtx.recSrc.LogicalAddress >= hlog.BeginAddress)
                    {
                        // Disk Region: Need to issue async io requests. Locking will be check on pending completion.
                        status = OperationStatus.RECORD_ON_DISK;
                        latchDestination = LatchDestination.CreatePendingContext;
                        goto CreatePendingContext;
                    }
                    else
                    {
                        // No record exists - check for lock before creating new record.
                        Debug.Assert(!fasterSession.IsManualLocking || LockTable.IsLockedExclusive(ref key, stackCtx.hei.hash), "A Lockable-session RMW() of an on-disk or non-existent key requires a LockTable lock");
                        if (LockTable.IsActive && !fasterSession.DisableEphemeralLocking 
                                && !LockTable.TryLockEphemeral(ref key, stackCtx.hei.hash, LockType.Exclusive, out stackCtx.recSrc.HasLockTableLock))
                        {
                            status = OperationStatus.RETRY_LATER;
                            goto LatchRelease;
                        }
                        goto CreateNewRecord;
                    }
                }
                else if (latchDestination == LatchDestination.Retry)
                {
                    goto LatchRelease;
                }

            #endregion Normal processing

            #region Lock source record
            LockSourceRecord:
                // This would be a local function to reduce "goto", but 'ref' variables and parameters aren't supported on local functions.
                srcRecordInfo = ref stackCtx.recSrc.GetSrcRecordInfo();
                if (!TryEphemeralXLock<Input, Output, Context, FasterSession>(fasterSession, ref stackCtx.recSrc, ref srcRecordInfo, out status))
                    goto LatchRelease;
                if (!srcRecordInfo.IsValidUpdateOrLockSource)
                {
                    EphemeralXUnlockAndAbandonUpdate<Input, Output, Context, FasterSession>(fasterSession, ref stackCtx.recSrc, ref srcRecordInfo);
                    srcRecordInfo = ref dummyRecordInfo;
                }
                goto CreateNewRecord;
            #endregion Lock source record

            #region Create new record
            CreateNewRecord:
                if (latchDestination != LatchDestination.CreatePendingContext)
                {
                    Value tempValue = default;
                    ref var value = ref (stackCtx.recSrc.HasInMemorySrc ? ref stackCtx.recSrc.GetSrcValue() : ref tempValue);

                    // Here, the input* data for 'doingCU' is the same as recSrc.
                    status = CreateNewRecordRMW(ref key, ref input, ref value, ref output, ref pendingContext, fasterSession, sessionCtx, ref stackCtx, ref srcRecordInfo,
                                                inputSrc: ref stackCtx.recSrc, inputRecordInfo: srcRecordInfo);
                    if (!OperationStatusUtils.IsAppend(status))
                    {
                        // OperationStatus.SUCCESS is OK here; it means NeedCopyUpdate or NeedInitialUpdate returned false
                        if (status == OperationStatus.ALLOCATE_FAILED && pendingContext.IsAsync || status == OperationStatus.RECORD_ON_DISK)
                        {
                            latchDestination = LatchDestination.CreatePendingContext;
                            goto CreatePendingContext;
                        }
                    }
                    goto LatchRelease;
                }
                #endregion Create new record
            }
            finally
            {
                stackCtx.HandleNewRecordOnError(this);
                EphemeralXUnlockAfterUpdate<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx, ref srcRecordInfo);
            }

        #region Create pending context
        CreatePendingContext:
            Debug.Assert(latchDestination == LatchDestination.CreatePendingContext, $"RMW CreatePendingContext encountered latchDest == {latchDestination}");
            {
                pendingContext.type = OperationType.RMW;
                if (pendingContext.key == default)
                    pendingContext.key = hlog.GetKeyContainer(ref key);
                if (pendingContext.input == default)
                    pendingContext.input = fasterSession.GetHeapContainer(ref input);

                pendingContext.output = output;
                if (pendingContext.output is IHeapConvertible heapConvertible)
                    heapConvertible.ConvertToHeap();

                pendingContext.userContext = userContext;
                pendingContext.PrevLatestLogicalAddress = stackCtx.recSrc.LatestLogicalAddress;   // InternalContinuePendingRMW compares to this to see if a new record was spliced in
                pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
                pendingContext.version = sessionCtx.version;
                pendingContext.serialNum = lsn;
                pendingContext.PrevHighestKeyHashAddress = prevHighestKeyHashAddress;
            }
        #endregion

        #region Latch release
        LatchRelease:
            if (latchOperation != LatchOperation.None)
            {
                switch (latchOperation)
                {
                    case LatchOperation.Shared:
                        HashBucket.ReleaseSharedLatch(stackCtx.hei.bucket);
                        break;
                    case LatchOperation.Exclusive:
                        HashBucket.ReleaseExclusiveLatch(stackCtx.hei.bucket);
                        break;
                    default:
                        break;
                }
            }
            #endregion

            return status;
        }

        private LatchDestination AcquireLatchRMW<Input, Output, Context>(PendingContext<Input, Output, Context> pendingContext, FasterExecutionContext<Input, Output, Context> sessionCtx,
                                                                         ref HashEntryInfo hei, ref OperationStatus status, ref LatchOperation latchOperation, long logicalAddress)
        {
            switch (sessionCtx.phase)
            {
                case Phase.PREPARE:
                    {
                        if (HashBucket.TryAcquireSharedLatch(hei.bucket))
                        {
                            // Set to release shared latch (default)
                            latchOperation = LatchOperation.Shared;
                            if (CheckBucketVersionNew(ref hei.entry))
                            {
                                status = OperationStatus.CPR_SHIFT_DETECTED;
                                return LatchDestination.Retry; // Pivot Thread for retry
                            }
                            break; // Normal Processing
                        }
                        else
                        {
                            status = OperationStatus.CPR_SHIFT_DETECTED;
                            return LatchDestination.Retry; // Pivot Thread for retry
                        }
                    }
                case Phase.IN_PROGRESS:
                    {
                        if (!CheckEntryVersionNew(logicalAddress))
                        {
                            if (HashBucket.TryAcquireExclusiveLatch(hei.bucket))
                            {
                                // Set to release exclusive latch (default)
                                latchOperation = LatchOperation.Exclusive;
                                if (logicalAddress >= hlog.HeadAddress)
                                    return LatchDestination.CreateNewRecord; // Create a (v+1) record
                            }
                            else
                            {
                                status = OperationStatus.RETRY_LATER;
                                return LatchDestination.Retry; // Refresh and retry
                            }
                        }
                        break; // Normal Processing
                    }
                case Phase.WAIT_INDEX_CHECKPOINT:
                case Phase.WAIT_FLUSH:
                    {
                        if (!CheckEntryVersionNew(logicalAddress))
                        {
                            if (logicalAddress >= hlog.HeadAddress)
                                return LatchDestination.CreateNewRecord; // Create a (v+1) record
                        }
                        break; // Normal Processing
                    }
                default:
                    break;
            }
            return LatchDestination.NormalProcessing;
        }

        /// <summary>
        /// Create a new record for RMW
        /// </summary>
        /// <typeparam name="Input"></typeparam>
        /// <typeparam name="Output"></typeparam>
        /// <typeparam name="Context"></typeparam>
        /// <typeparam name="FasterSession"></typeparam>
        /// <param name="key">The record Key</param>
        /// <param name="input">Input to the operation</param>
        /// <param name="value">Old value</param>
        /// <param name="output">The result of IFunctions.SingleWriter</param>
        /// <param name="pendingContext">Information about the operation context</param>
        /// <param name="fasterSession">The current session</param>
        /// <param name="sessionCtx">The current session context</param> // TODO can this be replaced with fasterSession.clientSession.ctx?
        /// <param name="stackCtx">Contains the <see cref="HashEntryInfo"/> and <see cref="RecordSource{Key, Value}"/> structures for this operation,
        ///     and allows passing back the newLogicalAddress for invalidation in the case of exceptions.</param>
        /// <param name="srcRecordInfo">If <paramref name="stackCtx"/>.<see cref="RecordSource{Key, Value}.HasInMemorySrc"/>,
        ///     this is the <see cref="RecordInfo"/> for <see cref="RecordSource{Key, Value}.LogicalAddress"/></param>
        /// <param name="inputSrc">If <paramref name="fromPending"/>, this is populated from the request record; otherwise it is <paramref name="stackCtx"/>.<see cref="RecordSource{Key, Value}"/></param>
        /// <param name="inputRecordInfo">If <paramref name="fromPending"/>, this is the <see cref="RecordInfo"/> for the request record.
        ///     Otherwise it is a copy of <paramref name="srcRecordInfo"/>.</param>
        /// <param name="fromPending">Whether we are being called from pending path</param>
        /// <remarks><paramref name="inputSrc"></paramref> vs. <paramref name="stackCtx"/> is a critically important distinction for pending RMW:
        /// <list type="bullet">
        ///     <item><b>NonPending</b>: <paramref name="stackCtx"/> is the usual source for locking and copying; <paramref name="inputSrc"/> is an alias</item>
        ///     <item><b>Pending</b>: <paramref name="stackCtx"/> is the source for locking *only*; <paramref name="inputSrc"/> contains the property valus for actual data read from disk.
        ///         In particular:
        ///         <list type="bullet">
        ///             <item><paramref name="inputSrc"/>.<see cref="RecordSource{Key, Value}.Log"/> always is hlog; <paramref name="stackCtx"/>.<see cref="RecordSource{Key, Value}.Log"/>
        ///                 may be the readcache. Cross-log address accesses are a Bad Thing.</item>
        ///             <item><paramref name="inputSrc"/>.<see cref="RecordSource{Key, Value}.HasInMemorySrc"/> reflects the request's logicalAddress rather than the locking record's</item>
        ///             <item><paramref name="inputSrc"/> has no readcache or LockTable information.</item>
        ///         </list>
        ///         Therefore, for Pending RMW it is important to use <paramref name="inputSrc"/> rather than <paramref name="stackCtx"/> for all operations on input data.</item>
        /// </list>
        /// </remarks>
        /// <returns></returns>
        private OperationStatus CreateNewRecordRMW<Input, Output, Context, FasterSession>(ref Key key, ref Input input, ref Value value, ref Output output,
                                                                                          ref PendingContext<Input, Output, Context> pendingContext, FasterSession fasterSession,
                                                                                          FasterExecutionContext<Input, Output, Context> sessionCtx, 
                                                                                          ref OperationStackContext<Key, Value> stackCtx, ref RecordInfo srcRecordInfo, 
                                                                                          ref RecordSource<Key, Value> inputSrc, RecordInfo inputRecordInfo, bool fromPending = false)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            bool forExpiration = false;

            // Alias this here
            var doingCU = inputSrc.HasInMemorySrc && !srcRecordInfo.Tombstone;

        RetryNow:

            RMWInfo rmwInfo = new()
            {
                SessionType = fasterSession.SessionType,
                Version = sessionCtx.version,
                SessionID = sessionCtx.sessionID,
                Address = inputSrc.HasMainLogSrc ? inputSrc.LogicalAddress : Constants.kInvalidAddress,
                KeyHash = stackCtx.hei.hash
            };

            // Perform Need*
            if (doingCU)
            {
                rmwInfo.RecordInfo = inputRecordInfo;
                if (!fasterSession.NeedCopyUpdate(ref key, ref input, ref value, ref output, ref rmwInfo))
                {
                    if (rmwInfo.Action == RMWAction.CancelOperation)
                        return OperationStatus.CANCELED;
                    else if (rmwInfo.Action == RMWAction.ExpireAndResume)
                    {
                        doingCU = false;
                        forExpiration = true;
                    }
                    else
                        return OperationStatus.SUCCESS;
                }
            }

            if (!doingCU)
            {
                rmwInfo.RecordInfo = default;   // There is no existing record
                if (!fasterSession.NeedInitialUpdate(ref key, ref input, ref output, ref rmwInfo))
                    return rmwInfo.Action == RMWAction.CancelOperation ? OperationStatus.CANCELED : OperationStatus.NOTFOUND;
            }

            // Allocate and initialize the new record
            var (actualSize, allocatedSize) = doingCU ?
                inputSrc.Log.GetRecordSize(inputSrc.PhysicalAddress, ref input, fasterSession) :
                hlog.GetInitialRecordSize(ref key, ref input, fasterSession);

            var status = OperationStatus.SUCCESS;
            if (!GetAllocationForRetry(ref pendingContext, stackCtx.hei.Address, allocatedSize, out long newLogicalAddress, out long newPhysicalAddress))
            {
                // Spin to make sure newLogicalAddress is > recSrc.LatestLogicalAddress (the .PreviousAddress and CAS comparison value).
                do
                {
                    if (!BlockAllocate(allocatedSize, out newLogicalAddress, ref pendingContext, out status))
                        return status;
                    newPhysicalAddress = hlog.GetPhysicalAddress(newLogicalAddress);

                    if (!VerifyInMemoryAddresses(ref stackCtx, stackCtx.recSrc.HasReadCacheSrc ? stackCtx.recSrc.LogicalAddress | Constants.kReadCacheBitMask : Constants.kInvalidAddress))
                    {
                        SaveAllocationForRetry(ref pendingContext, newLogicalAddress, newPhysicalAddress, allocatedSize);
                        return OperationStatus.RETRY_NOW;   // If this failed, we have just gone through an epoch refresh, so don't need RETRY_LATER
                    }
                } while (newLogicalAddress < stackCtx.recSrc.LatestLogicalAddress);
            }

            ref RecordInfo newRecordInfo = ref WriteTentativeInfo(ref key, hlog, newPhysicalAddress, inNewVersion: sessionCtx.InNewVersion, tombstone: false, stackCtx.recSrc.LatestLogicalAddress);
            stackCtx.newLogicalAddress = newLogicalAddress;

            rmwInfo.Address = newLogicalAddress;
            rmwInfo.KeyHash = stackCtx.hei.hash;

            // Populate the new record
            rmwInfo.RecordInfo = newRecordInfo;
            if (!doingCU)
            {
                if (fasterSession.InitialUpdater(ref key, ref input, ref hlog.GetValue(newPhysicalAddress, newPhysicalAddress + actualSize), ref output, ref newRecordInfo, ref rmwInfo))
                {
                    status = forExpiration
                        ? OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.CreatedRecord | StatusCode.Expired)
                        : OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.CreatedRecord);
                }
                else
                {
                    if (rmwInfo.Action == RMWAction.CancelOperation)
                        return OperationStatus.CANCELED;
                    return OperationStatus.NOTFOUND | (forExpiration ? OperationStatus.EXPIRED : OperationStatus.NOTFOUND);
                }
            }
            else
            {
                ref Value newRecordValue = ref hlog.GetValue(newPhysicalAddress, newPhysicalAddress + actualSize);
                if (fasterSession.CopyUpdater(ref key, ref input, ref value, ref newRecordValue, ref output, ref newRecordInfo, ref rmwInfo))
                {
                    status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.CopyUpdatedRecord);
                    goto DoCAS;
                }
                if (rmwInfo.Action == RMWAction.CancelOperation)
                {
                    // TODO: Another area where we can stash the record for reuse (not retry; this is canceling of the current operation).
                    stackCtx.SetNewRecordInvalid(ref newRecordInfo);
                    return OperationStatus.CANCELED;
                }
                if (rmwInfo.Action == RMWAction.ExpireAndStop)
                {
                    newRecordInfo.Tombstone = true;
                    status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.CreatedRecord | StatusCode.Expired | StatusCode.Expired);
                    goto DoCAS;
                }
                else if (rmwInfo.Action == RMWAction.ExpireAndResume)
                {
                    doingCU = false;
                    forExpiration = true;
                        
                    if (!ReinitializeExpiredRecord(ref key, ref input, ref newRecordValue, ref output, ref newRecordInfo, ref rmwInfo,
                                            newLogicalAddress, sessionCtx, fasterSession, isIpu: false, out status))
                    {
                        // An IPU was not (or could not) be done. Cancel if requested, else invalidate the allocated record and retry.
                        if (status == OperationStatus.CANCELED)
                            return status;

                        // TODO: Another area where we can stash the record for reuse (not retry; this may have been false because the record was too small).
                        stackCtx.SetNewRecordInvalid(ref newRecordInfo);
                        goto RetryNow;
                    }
                    goto DoCAS;
                }
                else
                    return OperationStatus.SUCCESS | (forExpiration ? OperationStatus.EXPIRED : OperationStatus.SUCCESS);
            }

        DoCAS:
            // Insert the new record by CAS'ing either directly into the hash entry or splicing into the readcache/mainlog boundary.
            rmwInfo.RecordInfo = newRecordInfo;
            bool success = CASRecordIntoChain(ref stackCtx, newLogicalAddress);
            if (success)
            {
                if (!CompleteTwoPhaseUpdate<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx, ref srcRecordInfo, ref newRecordInfo, out var lockStatus))
                    return lockStatus;

                // If IU, status will be NOTFOUND; return that.
                if (!doingCU)
                {
                    // If IU, status will be NOTFOUND. ReinitializeExpiredRecord has many paths but is straightforward so no need to assert here.
                    Debug.Assert(forExpiration || OperationStatus.NOTFOUND == OperationStatusUtils.BasicOpCode(status), $"Expected NOTFOUND but was {status}");
                    fasterSession.PostInitialUpdater(ref key, ref input, ref hlog.GetValue(newPhysicalAddress), ref output, ref newRecordInfo, ref rmwInfo);
                }
                else
                {
                    // Else it was a CopyUpdater so call PCU
                    fasterSession.PostCopyUpdater(ref key, ref input, ref value, ref hlog.GetValue(newPhysicalAddress), ref output, ref newRecordInfo, ref rmwInfo);
                }

                pendingContext.recordInfo = newRecordInfo;
                pendingContext.logicalAddress = newLogicalAddress;
                stackCtx.ClearNewRecordTentativeBitAtomic(ref newRecordInfo);
                return status;
            }

            // CAS failed
            stackCtx.SetNewRecordInvalid(ref newRecordInfo);
            rmwInfo.RecordInfo = newRecordInfo;
            ref Value insertedValue = ref hlog.GetValue(newPhysicalAddress);
            ref Key insertedKey = ref hlog.GetKey(newPhysicalAddress);
            if (!doingCU)
                fasterSession.DisposeInitialUpdater(ref insertedKey, ref input, ref insertedValue, ref output, ref newRecordInfo, ref rmwInfo);
            else
                fasterSession.DisposeCopyUpdater(ref insertedKey, ref input, ref value, ref insertedValue, ref output, ref newRecordInfo, ref rmwInfo);

            SaveAllocationForRetry(ref pendingContext, newLogicalAddress, newPhysicalAddress, allocatedSize);
            return OperationStatus.RETRY_NOW;   // CAS failure does not require epoch refresh
        }
    }
}
