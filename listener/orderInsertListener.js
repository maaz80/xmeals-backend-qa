import { createClient } from "@supabase/supabase-js";
import dotenv from "dotenv";
import { onOrderCreated } from "../controllers/orderController/orderController.js";

dotenv.config();

const supabaseRealtime = createClient(
     process.env.SUPABASE_URL,
     process.env.SUPABASE_SERVICE_ROLE_KEY,
     {
          realtime: {
               params: {
                    eventsPerSecond: 10,
               },
          },
          auth: {
               autoRefreshToken: false,
               persistSession: false,
          },
     }
);
let channel;

export function startOrderInsertListener() {
     if (channel) {
          console.log("⚠️ Order listener already running");
          return;
     }

     channel = supabaseRealtime
          .channel("orders-status-placed-channel")
          .on(
               "postgres_changes",
               {
                    event: "UPDATE",
                    schema: "public",
                    table: "orders",
               },
               async (payload) => {
                    const oldStatus = payload.old?.status;
                    const newStatus = payload.new?.status;
                    const waMessage = payload.new?.wa_message_created_ts;

                    // ✅ Sirf jab status change ho ke "Placed" bane
                    if (oldStatus !== "Placed" 
                         && newStatus === "Placed"
                         && waMessage === null
                    ) {
                         console.log("🟢 Order moved to PLACED:", payload.new.order_id);

                         await onOrderCreated(
                              {
                                   body: {
                                        order_id: payload.new.order_id,
                                        v_id: payload.new.v_id,
                                        user_order_id: payload.new.user_order_id,
                                   },
                              },
                              {
                                   status: () => ({ json: () => { } }),
                              }
                         );
                    }
               }
          )
          .subscribe((status) => {
               console.log("Realtime subscription status:", status);
          });
}
