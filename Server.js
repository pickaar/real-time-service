require('dotenv').config();

const http = require('http');
const express = require('express');
const mongoose = require('mongoose');
const { Server } = require('socket.io');
const { getQuoteById, setupQuoteService } = require('./service/getQuote');

const app = express();
const server = http.createServer(app);

const MONGO_URI = process.env.MONGODB_URI;
const PORT = process.env.PORT || 3002;
const SOCKET_BOOKING_ROOM = process.env.SOCKET_BOOKING_ROOM || 'join_booking_room';
const SOCKET_EVENT_UPDATE = process.env.SOCKET_EVENT_UPDATE || 'booking_quote_update';

const io = new Server(server, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST'],
    },
});

const customerRooms = new Map();

const getUniqueRoomKey = (bookingId, type) => `${bookingId}_${type}`;

const handleDbChange = async (changeEvent) => {
    if (!changeEvent.fullDocument || !changeEvent.fullDocument.bookingRefId || !changeEvent.fullDocument.bookingType) {
        console.warn("Change event is missing required fields (fullDocument, bookingRefId, or bookingType).", changeEvent);
        return;
    }
    console.log("Received change event:", changeEvent.fullDocument);
    const bookingId = changeEvent.fullDocument.bookingRefId.toString();
    const bookingType = changeEvent.fullDocument.bookingType.toString();
    const uniqueRoomKey = getUniqueRoomKey(bookingId, bookingType);
    const quoteObj = changeEvent.fullDocument;
    console.log(`Change detected for booking ${bookingId}. Emitting to room: ${uniqueRoomKey}`);

    try {
        const payload = await getQuoteById(bookingId, quoteObj);
        if (payload) {
            io.to(uniqueRoomKey).emit(SOCKET_EVENT_UPDATE, payload);
            console.log(`Update sent to room: ${uniqueRoomKey}`);
        } else {
            console.warn(`No payload generated for bookingId: ${bookingId}`);
        }
    } catch (error) {
        console.error(`Error fetching quote for bookingId ${bookingId}:`, error);
    }
};

async function startChangeStream() {
    try {
        const client = mongoose.connection.getClient();
        const db = client.db('bookings_db');
        const quoteCollection = db.collection('vehicle_bookings_quotes');

        const changeStream = quoteCollection.watch(
            [{ $match: { operationType: { $in: ['insert', 'replace', 'update'] } } }],
            { fullDocument: 'updateLookup' }
        );

        changeStream.on('change', handleDbChange);
        changeStream.on('error', (error) => {
            console.error('MongoDB Change Stream error:', error);
        });

        console.log("MongoDB Change Stream is listening...");
    } catch (err) {
        console.error('Error starting change stream:', err);
    }
}

io.on('connection', (socket) => {
    console.log(`Client connected: ${socket.id}`);

    socket.on(SOCKET_BOOKING_ROOM, (payload) => {
        const { id, type } = payload || {};
        if (!id || !type) {
            console.log(`Join failed for socket ${socket.id}: Missing id or type in payload.`);
            return;
        }

        const uniqueRoomKey = getUniqueRoomKey(id, type);
        socket.join(uniqueRoomKey);
        console.log(`Socket ${socket.id} joined room ${uniqueRoomKey}`);

        if (!customerRooms.has(uniqueRoomKey)) {
            customerRooms.set(uniqueRoomKey, new Set());
        }
        customerRooms.get(uniqueRoomKey).add(socket.id);
    });

    socket.on('disconnect', () => {
        console.log(`Client disconnected: ${socket.id}`);
        customerRooms.forEach((sockets, room) => {
            if (sockets.has(socket.id)) {
                sockets.delete(socket.id);
                if (sockets.size === 0) {
                    customerRooms.delete(room);
                    console.log(`Room ${room} is now empty and has been removed.`);
                }
            }
        });
    });
});

async function startServer() {
    try {
        await mongoose.connect(MONGO_URI);
        console.log('✅ Mongoose connected successfully!');

        setupQuoteService();
        await startChangeStream();

        server.listen(PORT, () => {
            console.log(`Real-Time Service listening on port ${PORT}`);
        });
    } catch (error) {
        console.error('❌ FATAL ERROR: Server failed to start:', error);
        process.exit(1);
    }
}

startServer();
