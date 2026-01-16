import express from "express";
import { 
  reverseGeocode, 
  getAddressFromPlaceId 
} from "../controllers/googleControllers/geocodingController.js";

const router = express.Router();

/**
 * @route GET /reverse-geocode
 * @description Reverse geocode coordinates to get address information
 * @param {string} lat - Latitude coordinate
 * @param {string} lng - Longitude coordinate
 * @returns {Object} Google Maps geocoding response
 */
router.get("/reverse-geocode", reverseGeocode);

/**
 * @route GET /address-from-placeid
 * @description Get detailed address information from Google Places API using place ID
 * @param {string} placeId - Google Places place ID
 * @returns {Object} Formatted address information
 */
router.get("/address-from-placeid", getAddressFromPlaceId);

export default router;
