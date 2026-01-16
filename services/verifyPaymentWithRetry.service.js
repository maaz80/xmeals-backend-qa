import { verifyPaymentCore } from "../services/verifyPayment.core.js";

export const verifyPaymentWithRetry = async ({
     supabase,
     rpcParams
}) => {
     const delays = [0, 10000, 15000];
     let lastError;

     for (let attempt = 0; attempt < delays.length; attempt++) {
          try {
               if (delays[attempt] > 0) {
                    console.log(`⏳ Retry attempt ${attempt} after ${delays[attempt]}ms delay`);
                    await new Promise(r => setTimeout(r, delays[attempt]));
               }

               return await verifyPaymentCore({
                    supabase,
                    rpcParams
               });

          } catch (err) {
               lastError = err;
          }
     }

     return { data: null, error: lastError };
};
