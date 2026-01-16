// orderTimeWorker.js
import cron from "node-cron";
import { supabase } from "../../config/supbase.js";
import { sendTextMessage, sendWhatsappTemplate } from "./orderController.js";
import { assertVendorAuthorized, calculateFinalAmount, getFullOrderDetails } from "../../services/orderService.js";

// har 1 minute me chalega
cron.schedule("*/3 * * * *", async () => {
     console.log("[CRON] Checking orders for start-preparing reminder...");

     const { data: orders, error } = await supabase
          .from("orders")
          .select("*")
          .eq("status", "accepted"); // sirf accepted

     if (error) {
          console.error("fetch orders error", error);
          return;
     }

     const now = new Date();

     for (const o of orders) {
          if (!o.accepted_ts) continue; // accepted_ts zaruri
          if (o.wa_preparing_reminder_sent) continue;

          const dpAssigned = !!o.dp_id;

          const createdTs = new Date(o.created_ts);
          const etaTs = new Date(o.eta);
          const now = new Date();

          const totalTime = etaTs - createdTs;
          const timePassed = now - createdTs;

          const percentagePassed =
               totalTime > 0 ? (timePassed / totalTime) * 100 : 100;

          const travelTimeInMs = Number(o.travel_time || 0) * 60 * 1000;
          const remainingTime = etaTs - now;
          const allowDueToTravelTime = remainingTime <= travelTimeInMs;

          // 🔁 SAME CONDITION as frontend
          const allow =
               dpAssigned || percentagePassed >= 65 || allowDueToTravelTime;

          if (!allow) continue;

          console.log("[CRON] Sending start-preparing reminder for", o.order_id);

          const order_id = o.order_id;
          const user_order_id = o.user_order_id

          // 1️⃣ Full order details (order, vendor, user, itemsText)
          const { order, vendor, user, itemsText } = await getFullOrderDetails(order_id);

          const to = vendor.mobile_number.replace(/\D/g, "");
          const { final_amount } = calculateFinalAmount(order);
          const displayOrderId = String(user_order_id || o.user_order_id || "");

               // Authorization check
          const allowed = await assertVendorAuthorized(order_id, to);
          if (!allowed) {
               console.log("Unauthorized WhatsApp user for order", displayOrderId, to);
               await sendTextMessage({
                    to: to,
                    text: "❌ You are not authorized to manage this order. Please contact support or use your registered WhatsApp number.",
               });
               continue;
          }

          // 2️⃣ Start Preparing template
          await sendWhatsappTemplate({
               to,
               templateName: "order_preparing", // WhatsApp template name
               bodyParams: [
                    { type: "text", text: displayOrderId },             // {{1}}
                    { type: "text", text: String(final_amount) }, // {{2}}
                    { type: "text", text: user.name },                  // {{3}}
                    { type: "text", text: itemsText },                  // {{4}}
               ],
               buttonPayload: `START_PREPARING:${order.order_id}`,
          });

          // 3️⃣ Dobara na bhejne ke liye status change
          await supabase
               .from("orders")
               .update({ wa_preparing_reminder_sent: true })
               .eq("order_id", order.order_id);
     }
});
