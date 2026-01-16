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

export function startOrderInsertListener() {
     supabaseRealtime
          .channel("orders-insert-channel")
          .on(
               "postgres_changes",
               {
                    event: "INSERT",
                    schema: "public",
                    table: "orders",
               },
               async (payload) => {
                    console.log("🟢 Order INSERT realtime:", payload.new);

                    // backend logic reuse
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
          )
          .subscribe((status) => {
               console.log("Realtime subscription status:", status);
          });
}
