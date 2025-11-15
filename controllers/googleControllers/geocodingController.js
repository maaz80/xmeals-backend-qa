import axios from "axios";

/**
 * Reverse geocode coordinates to get address information
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export const reverseGeocode = async (req, res) => {
  try {
    const { lat, lng } = req.query;

    if (!lat || !lng) {
      return res.status(400).json({ 
        error: "Missing required parameters: lat and lng" 
      });
    }

    const apiKey = process.env.GOOGLE_MAPS_API_KEY;

    if (!apiKey) {
      return res.status(500).json({ 
        error: "Google Maps API key not configured" 
      });
    }

    const response = await axios.get(
      `https://maps.googleapis.com/maps/api/geocode/json`,
      {
        params: {
          latlng: `${lat},${lng}`,
          key: apiKey,
        },
      }
    );

    res.json(response.data);

  } catch (error) {
    console.error("Reverse Geocoding Error:", {
      message: error.message,
      response: error.response?.data,
      status: error.response?.status,
    });

    res.status(500).json({
      message: 'Error fetching location data',
      error: {
        message: error.message,
        response: error.response?.data,
        status: error.response?.status,
      },
    });
  }
};

/**
 * Get address details from Google Places API using place ID
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export const getAddressFromPlaceId = async (req, res) => {
  try {
    const { placeId } = req.query;

    if (!placeId) {
      return res.status(400).json({ 
        error: "Missing placeId parameter" 
      });
    }

    const apiKey = process.env.GOOGLE_MAPS_API_KEY;

    if (!apiKey) {
      return res.status(500).json({ 
        error: "Google Maps API key not configured" 
      });
    }

    const googleRes = await axios.get(
      `https://maps.googleapis.com/maps/api/place/details/json`,
      {
        params: {
          place_id: placeId,
          key: apiKey,
        },
      }
    );

    const data = googleRes.data;

    if (data.status !== "OK") {
      return res.status(400).json({ 
        error: `Google Places API error: ${data.status}` 
      });
    }

    const result = data.result;
    const addressComponents = result.address_components;
    const formattedAddress = result.formatted_address;
    const location = result.geometry.location;

    // Helper function to extract address component by type
    const getComponent = (types) =>
      addressComponents.find((component) =>
        types.every((type) => component.types.includes(type))
      )?.long_name || "";

    // Extract address components
    const city = getComponent(["locality"]);
    const state = getComponent(["administrative_area_level_1"]);
    const country = getComponent(["country"]);
    const postalCode = getComponent(["postal_code"]);

    // Return formatted response
    res.json({
      full_address: formattedAddress,
      city,
      state,
      country,
      pincode: postalCode,
      latitude: location.lat,
      longitude: location.lng,
    });

  } catch (error) {
    console.error("Place Details Error:", {
      message: error.message,
      response: error.response?.data,
      status: error.response?.status,
    });

    res.status(500).json({ 
      error: "Internal Server Error",
      message: error.message 
    });
  }
};
