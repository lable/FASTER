﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool HandleImmediateRetryStatus<Input, Output, Context, FasterSession>(
            OperationStatus internalStatus,
            FasterExecutionContext<Input, Output, Context> opCtx,
            FasterExecutionContext<Input, Output, Context> currentCtx,
            FasterSession fasterSession,
            ref PendingContext<Input, Output, Context> pendingContext)
            where FasterSession : IFasterSession
            => (internalStatus & OperationStatus.BASIC_MASK) > OperationStatus.MAX_MAP_TO_COMPLETED_STATUSCODE
                && HandleRetryStatus(internalStatus, opCtx, currentCtx, fasterSession, ref pendingContext);

        /// <summary>
        /// Handle retry for operations that will not go pending (e.g., InternalLock)
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool HandleImmediateNonPendingRetryStatus<Input, Output, Context, FasterSession>(OperationStatus internalStatus, FasterExecutionContext<Input, Output, Context> currentCtx, FasterSession fasterSession)
            where FasterSession : IFasterSession
        {
            Debug.Assert(epoch.ThisInstanceProtected());
            switch (internalStatus)
            {
                case OperationStatus.RETRY_NOW:
                    Thread.Yield();
                    return true;
                case OperationStatus.RETRY_LATER:
                    InternalRefresh(currentCtx, fasterSession);
                    Thread.Yield();
                    return true;
                default:
                    return false;
            }
        }

        private bool HandleRetryStatus<Input, Output, Context, FasterSession>(
            OperationStatus internalStatus,
            FasterExecutionContext<Input, Output, Context> opCtx,
            FasterExecutionContext<Input, Output, Context> currentCtx,
            FasterSession fasterSession,
            ref PendingContext<Input, Output, Context> pendingContext)
            where FasterSession : IFasterSession
        {
            Debug.Assert(epoch.ThisInstanceProtected());
            switch (internalStatus)
            {
                case OperationStatus.RETRY_NOW:
                    Thread.Yield();
                    return true;
                case OperationStatus.RETRY_LATER:
                    InternalRefresh(currentCtx, fasterSession);
                    pendingContext.version = currentCtx.version;
                    Thread.Yield();
                    return true;
                case OperationStatus.CPR_SHIFT_DETECTED:
                    // Retry as (v+1) Operation
                    SynchronizeEpoch(opCtx, currentCtx, ref pendingContext, fasterSession);
                    return true;
                case OperationStatus.ALLOCATE_FAILED:
                    // Async handles this in its own way, as part of the *AsyncResult.Complete*() sequence.
                    Debug.Assert(!pendingContext.flushEvent.IsDefault(), "flushEvent is required for ALLOCATE_FAILED");
                    if (pendingContext.IsAsync)
                        return false;
                    try
                    {
                        epoch.Suspend();
                        pendingContext.flushEvent.Wait();
                    }
                    finally
                    {
                        pendingContext.flushEvent = default;
                        epoch.Resume();
                    }
                    return true;
                default:
                    return false;
            }
        }

        /// <summary>
        /// Performs appropriate handling based on the internal failure status of the trial.
        /// </summary>
        /// <param name="opCtx">Thread (or session) context under which operation was tried to execute.</param>
        /// <param name="pendingContext">Internal context of the operation.</param>
        /// <param name="operationStatus">Internal status of the trial.</param>
        /// <returns>Operation status</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status HandleOperationStatus<Input, Output, Context>(
            FasterExecutionContext<Input, Output, Context> opCtx,
            ref PendingContext<Input, Output, Context> pendingContext,
            OperationStatus operationStatus)
        {
            if (OperationStatusUtils.TryConvertToCompletedStatusCode(operationStatus, out Status status))
                return status;
            return HandleOperationStatus(opCtx, ref pendingContext, operationStatus, out _);
        }

        /// <summary>
        /// Performs appropriate handling based on the internal failure status of the trial.
        /// </summary>
        /// <param name="opCtx">Thread (or session) context under which operation was tried to execute.</param>
        /// <param name="pendingContext">Internal context of the operation.</param>
        /// <param name="operationStatus">Internal status of the trial.</param>
        /// <param name="request">IO request, if operation went pending</param>
        /// <returns>Operation status</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status HandleOperationStatus<Input, Output, Context>(
            FasterExecutionContext<Input, Output, Context> opCtx,
            ref PendingContext<Input, Output, Context> pendingContext,
            OperationStatus operationStatus,
            out AsyncIOContext<Key, Value> request)
        {
            Debug.Assert(operationStatus != OperationStatus.RETRY_NOW, "OperationStatus.RETRY_NOW should have been handled before HandleOperationStatus");
            Debug.Assert(operationStatus != OperationStatus.RETRY_LATER, "OperationStatus.RETRY_LATER should have been handled before HandleOperationStatus");
            Debug.Assert(operationStatus != OperationStatus.CPR_SHIFT_DETECTED, "OperationStatus.CPR_SHIFT_DETECTED should have been handled before HandleOperationStatus");

            request = default;

            if (OperationStatusUtils.TryConvertToCompletedStatusCode(operationStatus, out Status status))
                return status;

            if (operationStatus == OperationStatus.ALLOCATE_FAILED)
            {
                Debug.Assert(pendingContext.IsAsync, "Sync ops should have handled ALLOCATE_FAILED before HandleOperationStatus");
                Debug.Assert(!pendingContext.flushEvent.IsDefault(), "Expected flushEvent for ALLOCATE_FAILED");
                return new(StatusCode.Pending);
            }
            else if (operationStatus == OperationStatus.RECORD_ON_DISK)
            {
                Debug.Assert(pendingContext.flushEvent.IsDefault(), "Cannot have flushEvent with RECORD_ON_DISK");
                // Add context to dictionary
                pendingContext.id = opCtx.totalPending++;
                opCtx.ioPendingRequests.Add(pendingContext.id, pendingContext);

                // Issue asynchronous I/O request
                request.id = pendingContext.id;
                request.request_key = pendingContext.key;
                request.logicalAddress = pendingContext.logicalAddress;
                request.minAddress = pendingContext.minAddress;
                request.record = default;
                if (pendingContext.IsAsync)
                    request.asyncOperation = new TaskCompletionSource<AsyncIOContext<Key, Value>>(TaskCreationOptions.RunContinuationsAsynchronously);
                else
                    request.callbackQueue = opCtx.readyResponses;

                hlog.AsyncGetFromDisk(pendingContext.logicalAddress, hlog.GetAverageRecordSize(), request);
                return new(StatusCode.Pending);
            }
            else
            {
                Debug.Assert(pendingContext.IsAsync, "Sync ops should never return status.IsFaulted");
                return new(StatusCode.Error);
            }
        }
    }
}
