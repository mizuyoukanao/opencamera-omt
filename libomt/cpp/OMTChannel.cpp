/**
 * OMTChannel.cpp - Client Connection Channel Implementation
 * Port of libomtnet/src/OMTChannel.cs
 */

#include "OMTChannel.h"
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <sys/select.h>

#ifdef __ANDROID__
#include <android/log.h>
#define LOG_TAG "omt-channel"
#define LOGI(...) __android_log_print(ANDROID_LOG_INFO, LOG_TAG, __VA_ARGS__)
#define LOGE(...) __android_log_print(ANDROID_LOG_ERROR, LOG_TAG, __VA_ARGS__)
#define LOGD(...) __android_log_print(ANDROID_LOG_DEBUG, LOG_TAG, __VA_ARGS__)
#else
#define LOGI(...) 
#define LOGE(...)
#define LOGD(...)
#endif

namespace omt {

Channel::Channel(int socketFd, struct sockaddr_in address, 
                 int sendBufferSize, IChannelListener* listener)
    : socketFd_(socketFd), address_(address), listener_(listener) {

    // Socket stays in blocking mode: recv is guarded by select() in
    // receiverLoop, and sends are bounded by SO_SNDTIMEO below. Toggling
    // O_NONBLOCK per call raced between sender/receiver threads.

    // Bound blocking sends so a stalled receiver is detected and dropped
    // instead of freezing the sender thread for the TCP retransmission
    // timeout (many minutes).
    struct timeval sndTimeout{5, 0};
    setsockopt(socketFd_, SOL_SOCKET, SO_SNDTIMEO, &sndTimeout, sizeof(sndTimeout));

    // TCP_NODELAY for low latency
    int one = 1;
    setsockopt(socketFd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    
    // Set send buffer
    setSendBuffer(sendBufferSize);
    
    // TCP keepalive
    setsockopt(socketFd_, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one));
    
    // TCP_KEEPIDLE = 5 seconds (matches official libomtnet)
#ifdef TCP_KEEPIDLE
    int keepIdle = 5;
    setsockopt(socketFd_, IPPROTO_TCP, TCP_KEEPIDLE, &keepIdle, sizeof(keepIdle));
#endif
    
    // Create async send pool
    sendPool_ = std::make_unique<AsyncPool>(
        Constants::NETWORK_ASYNC_COUNT, 
        Constants::NETWORK_ASYNC_BUFFER_AV);
    
    // Start threads
    // Start threads
    senderThread_ = std::thread(&Channel::senderLoop, this);
    receiverThread_ = std::thread(&Channel::receiverLoop, this);
    
    // Initial metadata handshake is now handled by Sender
    
    LOGI("Channel: connected %s:%d", getRemoteAddress().c_str(), getRemotePort());
}

Channel::~Channel() {
    LOGI("Channel: destroying");
    running_ = false;
    connected_ = false;
    
    // Shutdown socket to unblock threads
    if (socketFd_ > 0) {
        shutdown(socketFd_, SHUT_RDWR);
    }
    
    queueCV_.notify_all();
    
    if (receiverThread_.joinable()) {
        receiverThread_.join();
    }
    if (senderThread_.joinable()) {
        senderThread_.join();
    }
    
    // Clear pending queue
    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        while (!pendingQueue_.empty()) {
            sendPool_->release(pendingQueue_.front());
            pendingQueue_.pop();
        }
    }
    
    closeSocket();
    
    LOGI("Channel: destroyed, sent=%lld, dropped=%lld", 
         (long long)framesSent_.load(), (long long)framesDropped_.load());
}

void Channel::disconnect() {
    connected_ = false;
    running_ = false;
    closeSocket();
}

void Channel::closeSocket() {
    if (socketFd_ > 0) {
        close(socketFd_);
        socketFd_ = -1;
    }
}

void Channel::setSendBuffer(int size) {
    setsockopt(socketFd_, SOL_SOCKET, SO_SNDBUF, &size, sizeof(size));
    
    int actualBuf = 0;
    socklen_t optlen = sizeof(actualBuf);
    getsockopt(socketFd_, SOL_SOCKET, SO_SNDBUF, &actualBuf, &optlen);
    LOGI("Channel: SO_SNDBUF=%d (requested %d)", actualBuf, size);
}

std::string Channel::getRemoteAddress() const {
    char buf[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &address_.sin_addr, buf, sizeof(buf));
    return std::string(buf);
}

int Channel::getRemotePort() const {
    return ntohs(address_.sin_port);
}

ChannelStats Channel::getStats() const {
    return {
        framesSent_.load(),
        framesDropped_.load(),
        bytesSent_.load(),
        bytesReceived_.load()
    };
}

bool Channel::sendMetadataSync(const char* xml) {
    if (!connected_) return false;

    int len = strlen(xml);
    int totalLen = len + 1;

    FrameHeader header{};
    header.version = 1;
    header.frameType = static_cast<uint8_t>(FrameType::Metadata);
    header.timestamp = 0;
    header.metadataLength = static_cast<uint16_t>(totalLen);
    header.dataLength = totalLen;

    // Build the whole frame and send it under sendMutex_ so metadata can
    // never interleave with a video frame the sender thread is writing.
    std::vector<uint8_t> buf(sizeof(header) + totalLen);
    memcpy(buf.data(), &header, sizeof(header));
    memcpy(buf.data() + sizeof(header), xml, len);
    buf[sizeof(header) + len] = 0;

    return sendAllBlocking(buf.data(), buf.size());
}

int Channel::sendAsync(const void* headerData, size_t headerLen,
                       const void* extHeaderData, size_t extHeaderLen,
                       const void* payloadData, size_t payloadLen) {
    if (!connected_) return 0;
    
    size_t totalLen = headerLen + extHeaderLen + payloadLen;
    
    if (totalLen > static_cast<size_t>(Constants::VIDEO_MAX_SIZE)) {
        framesDropped_++;
        LOGE("sendAsync: frame too large %zu", totalLen);
        return 0;
    }
    
    AsyncBuffer* buf = sendPool_->acquire();
    if (!buf) {
        // Pool exhausted (network can't keep up): drop the OLDEST queued
        // frame and reuse its buffer, so the receiver always gets the
        // freshest video instead of a growing backlog of stale frames.
        std::lock_guard<std::mutex> lock(queueMutex_);
        if (!pendingQueue_.empty()) {
            buf = pendingQueue_.front();
            pendingQueue_.pop();
            framesDropped_++;
            recentDropped_++;
        }
    }
    if (!buf) {
        framesDropped_++;
        recentDropped_++;
        static int dropLog = 0;
        if (dropLog++ % 30 == 0) {
            LOGD("sendAsync: pool exhausted (dropped: %lld)",
                 (long long)framesDropped_.load());
        }
        return 0;
    }
    
    buf->resize(totalLen);
    
    size_t offset = 0;
    memcpy(buf->data.data() + offset, headerData, headerLen);
    offset += headerLen;
    memcpy(buf->data.data() + offset, extHeaderData, extHeaderLen);
    offset += extHeaderLen;
    memcpy(buf->data.data() + offset, payloadData, payloadLen);
    buf->length = totalLen;
    
    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        pendingQueue_.push(buf);
    }
    queueCV_.notify_one();
    
    return static_cast<int>(totalLen);
}

void Channel::senderLoop() {
    LOGI("senderLoop: started");
    
    while (running_ && connected_) {
        AsyncBuffer* buf = nullptr;
        
        {
            std::unique_lock<std::mutex> lock(queueMutex_);
            queueCV_.wait_for(lock, std::chrono::milliseconds(100), [this] {
                return !pendingQueue_.empty() || !running_ || !connected_;
            });
            
            if (!running_ || !connected_) break;
            if (pendingQueue_.empty()) continue;
            
            buf = pendingQueue_.front();
            pendingQueue_.pop();
        }
        
        if (!buf) continue;
        
        bool success = sendAllBlocking(buf->data.data(), buf->length);
        
        if (success) {
            framesSent_++;
            bytesSent_ += buf->length;
        }
        
        sendPool_->release(buf);
    }
    
    LOGI("senderLoop: exiting");
}

bool Channel::sendAllBlocking(const uint8_t* data, size_t length) {
    // Serialize all writers (video sender thread, metadata senders) so
    // frames never interleave on the wire.
    std::lock_guard<std::mutex> lock(sendMutex_);

    size_t remaining = length;
    const uint8_t* ptr = data;

    while (remaining > 0 && connected_) {
        ssize_t sent = send(socketFd_, ptr, remaining, MSG_NOSIGNAL);

        if (sent > 0) {
            ptr += sent;
            remaining -= sent;
        } else if (sent < 0) {
            if (errno == EINTR) continue;

            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // SO_SNDTIMEO expired without progress: receiver stalled
                LOGE("sendAllBlocking: send timed out, dropping stalled receiver");
            } else {
                LOGE("sendAllBlocking: error %d (%s)", errno, strerror(errno));
            }
            connected_ = false;
            return false;
        } else {
            connected_ = false;
            return false;
        }
    }

    return remaining == 0;
}

void Channel::receiverLoop() {
    LOGI("receiverLoop: started");
    
    std::vector<uint8_t> recvBuffer(65536);
    size_t bufferPos = 0;
    
    while (running_ && connected_) {
        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(socketFd_, &readfds);
        
        struct timeval tv{0, 100000};  // 100ms
        
        int sel = select(socketFd_ + 1, &readfds, nullptr, nullptr, &tv);
        
        if (sel < 0) {
            if (errno == EINTR) continue;
            connected_ = false;
            break;
        }
        
        if (sel == 0) continue;

        if (bufferPos >= recvBuffer.size()) {
            // Buffer full without a complete frame (grown below when a
            // frame announces its size) - should not happen, but never
            // call recv() with length 0: it returns 0 and would be
            // misread as a disconnect.
            LOGE("receiverLoop: receive buffer full, disconnecting");
            connected_ = false;
            break;
        }

        ssize_t received = recv(socketFd_, recvBuffer.data() + bufferPos,
                                recvBuffer.size() - bufferPos, 0);
        
        if (received <= 0) {
            if (received == 0) {
                LOGI("receiverLoop: connection closed by peer");
            } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
                LOGE("receiverLoop: recv error %d", errno);
            }
            connected_ = false;
            break;
        }
        
        bytesReceived_ += received;
        bufferPos += received;
        
        // Process complete frames
        while (bufferPos >= FrameHeader::SIZE) {
            auto* header = reinterpret_cast<FrameHeader*>(recvBuffer.data());
            
            if (header->version != 1) {
                LOGE("receiverLoop: invalid version %d", header->version);
                connected_ = false;
                break;
            }

            // Receivers only send small metadata frames; a huge or negative
            // length means the stream is desynced - disconnect rather than
            // spinning forever.
            if (header->dataLength < 0 ||
                header->dataLength > Constants::METADATA_FRAME_SIZE) {
                LOGE("receiverLoop: bad dataLength %d", header->dataLength);
                connected_ = false;
                break;
            }

            uint8_t frameType = header->frameType;
            int32_t dataLength = header->dataLength;
            size_t totalFrameLen = FrameHeader::SIZE + dataLength;

            if (totalFrameLen > recvBuffer.size()) {
                // Grow to fit the announced frame (header pointer is
                // invalidated by this - fields were copied above).
                recvBuffer.resize(totalFrameLen);
            }

            if (totalFrameLen > bufferPos) break;

            // Process metadata
            if (frameType == static_cast<uint8_t>(FrameType::Metadata) &&
                dataLength > 0) {
                char* xmlData = reinterpret_cast<char*>(recvBuffer.data() + FrameHeader::SIZE);

                // Fix for conflicting implementations:
                // - C# libomtnet sends XML *without* null terminator
                // - Protocol says null terminated
                // - C++ sender sends *with* null terminator
                size_t strLen = dataLength;
                if (strLen > 0 && xmlData[strLen - 1] == '\0') {
                    strLen--;
                }
                
                std::string xml(xmlData, strLen);
                processMetadata(xml);
            }
            
            // Shift buffer
            if (totalFrameLen < bufferPos) {
                memmove(recvBuffer.data(), recvBuffer.data() + totalFrameLen,
                        bufferPos - totalFrameLen);
            }
            bufferPos -= totalFrameLen;
        }
    }
    
    LOGI("receiverLoop: exiting");
    
    // Notify listener
    if (listener_ && !connected_) {
        listener_->onDisconnected(this);
    }
}

void Channel::processMetadata(const std::string& xml) {
    // Subscription commands
    if (xml == MetadataConstants::CHANNEL_SUBSCRIBE_VIDEO) {
        subscriptions_ = subscriptions_.load() | Subscription::Video;
        LOGI("Client subscribed to VIDEO");
        return;
    }
    if (xml == MetadataConstants::CHANNEL_SUBSCRIBE_AUDIO) {
        subscriptions_ = subscriptions_.load() | Subscription::Audio;
        LOGI("Client subscribed to AUDIO");
        return;
    }
    if (xml == MetadataConstants::CHANNEL_SUBSCRIBE_METADATA) {
        subscriptions_ = subscriptions_.load() | Subscription::Metadata;
        LOGI("Client subscribed to METADATA");
        return;
    }
    
    // Tally commands
    if (xml == MetadataConstants::TALLY_PREVIEWPROGRAM) {
        tallyPreview_ = true; tallyProgram_ = true;
        if (listener_) listener_->onTallyChanged(this);
        return;
    }
    if (xml == MetadataConstants::TALLY_PROGRAM) {
        tallyPreview_ = false; tallyProgram_ = true;
        if (listener_) listener_->onTallyChanged(this);
        return;
    }
    if (xml == MetadataConstants::TALLY_PREVIEW) {
        tallyPreview_ = true; tallyProgram_ = false;
        if (listener_) listener_->onTallyChanged(this);
        return;
    }
    if (xml == MetadataConstants::TALLY_NONE) {
        tallyPreview_ = false; tallyProgram_ = false;
        if (listener_) listener_->onTallyChanged(this);
        return;
    }
    
    // Preview mode
    if (xml == MetadataConstants::CHANNEL_PREVIEW_VIDEO_ON) {
        previewMode_ = true;
        return;
    }
    if (xml == MetadataConstants::CHANNEL_PREVIEW_VIDEO_OFF) {
        previewMode_ = false;
        return;
    }
    
    // Quality suggestion
    if (xml.find(MetadataConstants::SUGGESTED_QUALITY_PREFIX) == 0) {
        Quality oldQuality = suggestedQuality_.load();
        
        if (xml.find("\"Low\"") != std::string::npos) {
            suggestedQuality_ = Quality::Low;
        } else if (xml.find("\"Medium\"") != std::string::npos) {
            suggestedQuality_ = Quality::Medium;
        } else if (xml.find("\"High\"") != std::string::npos) {
            suggestedQuality_ = Quality::High;
        } else {
            suggestedQuality_ = Quality::Default;
        }
        
        if (suggestedQuality_.load() != oldQuality && listener_) {
            listener_->onQualityChanged(this);
        }
        return;
    }
    
    LOGD("Unknown metadata: %s", xml.c_str());
}

} // namespace omt
