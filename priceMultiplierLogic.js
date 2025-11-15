/**
 * Calculates a dynamic price multiplier for a food item based on several factors.
 * The multiplier is constrained to a range of 1.2 to 2.0.
 *
 * @param {object} inputs - The live, changing variables for the calculation.
 * @param {number} inputs.activeOrdersInArea - Current number of orders in the radius.
 * @param {number} inputs.vendorCurrentOrders - Number of orders being prepared by the restaurant.
 * @param {number} inputs.itemCurrentDemand - Number of times this item is in active orders.
 * @param {number} inputs.itemAverageRating - The item's average user rating (e.g., 1-5).
 * @param {number} inputs.numberOfRatings - The total count of ratings for the item.
 * @param {number} inputs.basePrice - The original price of the item.
 *
 * @param {object} config - The configuration constants for the business logic.
 * @param {number} config.maxAreaOrders - Benchmark for a "busy" area.
 * @param {number} config.maxVendorOrders - Max order capacity for a typical vendor.
 * @param {number} config.ratingConfidenceThreshold - Number of ratings for high confidence.
 * @param {number} config.priceCeiling - Price at or above which the price score is 0.
 * @param {object} config.weights - The weights for each factor, must sum to 1.0.
 * @param {number} config.weights.areaDemand - Weight for area demand score.
 * @param {number} config.weights.vendorLoad - Weight for vendor load score.
 * @param {number} config.weights.itemDemand - Weight for item demand score.
 * @param {number} config.weights.rating - Weight for rating score.
 * @param {number} config.weights.price - Weight for base price score.
 *
 * @returns {object} An object containing the final multiplier and all intermediate scores.
 */
function calculatePriceMultiplier(inputs, config) {
    // Destructure for easier access
    const {
        activeOrdersInArea: A,
        vendorCurrentOrders: V,
        itemCurrentDemand: I,
        itemAverageRating: R,
        numberOfRatings: N,
        basePrice: P
    } = inputs;

    const {
        maxAreaOrders: A_max,
        maxVendorOrders: V_max,
        ratingConfidenceThreshold: N_conf,
        priceCeiling: P_ceil,
        weights
    } = config;

    // --- 2. The Step-by-Step Calculation ---

    // Step 1: Area Demand Score (Score_A)
    const scoreA = Math.min(1, A / A_max);

    // Step 2: Vendor Load Score (Score_V)
    const scoreV = Math.min(1, V / V_max);

    // Step 3: Item Popularity Score (Score_I)
    const scoreI = (I > 0) ? (1 + Math.log10(I)) / 3 : 0;

    // Step 4: Rating Score (Score_R)
    const confidence = Math.min(1, N / N_conf);
    const scoreR = (R / 5) * confidence;

    // Step 5: Inverse Price Score (Score_P)
    const scoreP = Math.max(0, 1 - (P / P_ceil));

    // --- 3. Combine Scores and Calculate Final Multiplier ---

    // Step 6: Calculate the Combined Score
    const combinedScore =
        (weights.areaDemand * scoreA) +
        (weights.vendorLoad * scoreV) +
        (weights.itemDemand * scoreI) +
        (weights.rating * scoreR) +
        (weights.price * scoreP);

    // Step 7: Map Score to Final Multiplier (1.2 to 2.0)
    const multiplier = 1.2 + (combinedScore * 0.8);

    return {
        finalMultiplier: parseFloat(multiplier.toFixed(4)), // Return a clean, rounded number
        details: {
            combinedScore: parseFloat(combinedScore.toFixed(4)),
            scoreA: parseFloat(scoreA.toFixed(4)),
            scoreV: parseFloat(scoreV.toFixed(4)),
            scoreI: parseFloat(scoreI.toFixed(4)),
            scoreR: parseFloat(scoreR.toFixed(4)),
            scoreP: parseFloat(scoreP.toFixed(4)),
        }
    };
}

// --- Example Usage ---

// Define the configuration constants for your application
const CONFIG = {
    maxAreaOrders: 100,
    maxVendorOrders: 20,
    ratingConfidenceThreshold: 100,
    priceCeiling: 500,
    weights: {
        areaDemand: 0.25,
        vendorLoad: 0.20,
        itemDemand: 0.15,
        rating: 0.10,
        price: 0.30
    }
};

// Live variables for a "Paneer Tikka" order at a specific moment
const liveInputs = {
    activeOrdersInArea: 80,
    vendorCurrentOrders: 18,
    itemCurrentDemand: 12,
    itemAverageRating: 4.5,
    numberOfRatings: 150,
    basePrice: 220
};

// Calculate the result
const result = calculatePriceMultiplier(liveInputs, CONFIG);

// --- Display the results ---
console.log("--- Dynamic Pricing Calculation ---");
console.log(`Item Base Price: ₹${liveInputs.basePrice}`);
console.log("\n--- Scores ---");
console.log(`Area Demand Score: ${result.details.scoreA}`);
console.log(`Vendor Load Score: ${result.details.scoreV}`);
console.log(`Item Popularity Score: ${result.details.scoreI}`);
console.log(`Rating Score: ${result.details.scoreR}`);
console.log(`Inverse Price Score: ${result.details.scoreP}`);
console.log(`\nCombined Weighted Score: ${result.details.combinedScore}`);
console.log("\n--- Final Result ---");
console.log(`Price Multiplier: ${result.finalMultiplier}`);

const finalPrice = liveInputs.basePrice * result.finalMultiplier;
console.log(`New Dynamic Price: ₹${finalPrice.toFixed(2)}`);

/*
Expected Output:
--- Dynamic Pricing Calculation ---
Item Base Price: ₹220

--- Scores ---
Area Demand Score: 0.8
Vendor Load Score: 0.9
Item Popularity Score: 0.6931
Rating Score: 0.9
Inverse Price Score: 0.56

Combined Weighted Score: 0.742

--- Final Result ---
Price Multiplier: 1.7936
New Dynamic Price: ₹394.59
*/