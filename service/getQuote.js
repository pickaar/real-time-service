import axios from 'axios';
import mongoose from 'mongoose';
import { parseQuoteResponse } from '../util/responseQuoteParser.js';

let vehicleBookingsCollection;
let vehicleBookingsQuotesCollection;

const setupQuoteService = () => {
    const db = mongoose.connection.db;
    vehicleBookingsCollection = db.collection('vehicle_bookings');
    vehicleBookingsQuotesCollection = db.collection('vehicle_bookings_quotes');
    console.log("getQuote.js: Database collections initialized.");
};

const extractUniqueIds = (quoteList = []) => {
    const { vendorIds, vehicleIds } = quoteList.reduce((acc, quote) => {
        if (quote.vendorId) acc.vendorIds.add(quote.vendorId.toString());
        if (quote.vehicleId) acc.vehicleIds.add(quote.vehicleId.toString());
        return acc;
    }, { vendorIds: new Set(), vehicleIds: new Set() });

    return {
        vendorIds: Array.from(vendorIds),
        vehicleIds: Array.from(vehicleIds)
    };
};

const fetchUserMFEBatchData = async (vendorIds, vehicleIds) => {
  const response = await axios.post(`${process.env.USER_MICROSERVICE_URL}/vendor/user/batch-details`, {
    vendorIds,
    vehicleIds
  });
  return response.data.data;
};

async function getQuoteById(bookingId, quoteObj) {
    if (!bookingId) {
        console.log('No bookingId provided');
        return;
    }

    if (!vehicleBookingsQuotesCollection || !vehicleBookingsCollection) {
        console.error("Collections not initialized! Cannot fetch data.");
        return { status: 500, message: "Service not ready" };
    }

    const bookingDoc = await vehicleBookingsCollection.findOne({ _id: new mongoose.Types.ObjectId(bookingId) });
    const quotesList = quoteObj?.quotesList || [];

    if (quotesList.length === 0) {
        console.log('No quotes found for this booking');
        return { status: 200, message: 'No quotes found for this booking', data: [] };
    }

    const { vendorIds, vehicleIds } = extractUniqueIds(quotesList);
    const { vendorMap, vehicleMap } = await fetchUserMFEBatchData(vendorIds, vehicleIds);

    const augmentedQuotes = quotesList.map(quote => {
        const vendorIdString = quote.vendorId.toString();
        const vehicleIdString = quote.vehicleId.toString();

        const vendorDetails = vendorMap[vendorIdString] || { name: 'Unknown Vendor' };
        const vehicleDetails = vehicleMap[vehicleIdString] || { vehicleName: 'Unknown Vehicle' };

        return parseQuoteResponse(quote, vendorDetails, vehicleDetails, bookingDoc);
    });

    return {
        status: 200,
        message: 'Quotes retrieved successfully',
        data: augmentedQuotes,
    };
}

export { getQuoteById, setupQuoteService };
