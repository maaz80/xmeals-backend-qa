// services/orderService.js
import { supabase } from "../config/supbase.js";

export async function getFullOrderDetails(order_id) {
     // Order
     const { data: order, error: orderErr } = await supabase
          .from("orders")
          .select(`*,
      order_item: order_item_order_id_fkey(
               item_real_price,
               quantity
          )
    `)
          .eq("order_id", order_id)
          .single();
     if (orderErr || !order) throw orderErr || new Error("Order not found");

     // Vendor
     const { data: vendor, error: vendorErr } = await supabase
          .from("vendor_request")
          .select("*")
          .eq("v_id", order.v_id)
          .single();
     if (vendorErr || !vendor) throw vendorErr || new Error("Vendor not found");

     // User
     const { data: user, error: userErr } = await supabase
          .from("user")
          .select("name, mobile_number")
          .eq("user_id", order.u_id)
          .single();
     if (userErr || !user) throw userErr || new Error("User not found");

     // Items
     const { data: orderItems, error: itemsErr } = await supabase
          .from("order_item")
          .select("item_id, quantity, final_price")
          .eq("order_id", order_id);
     if (itemsErr) throw itemsErr;

     const itemIds = orderItems.map((i) => i.item_id);

     const { data: items, error: itemsDetailErr } = await supabase
          .from("item")
          .select("item_id, item_name")
          .in("item_id", itemIds);
     if (itemsDetailErr) throw itemsDetailErr;

     const itemsText = orderItems
          .map((oi) => {
               const it = items.find((x) => x.item_id === oi.item_id);
               const name = it?.item_name || "Item";
               return `${name} x${oi.quantity}`;
          })
          .join(", ");

     return { order, vendor, user, orderItems, items, itemsText };
}

/**
 * ✅ Check: kya ye WhatsApp number iss order ke vendor ka hai?
 * @param {string} order_id
 * @param {string} waId  - message.from (e.g. "9198xxxxxxx")
 * @returns {boolean}
 */
/**
 * @returns {Promise<boolean>}
 */
export async function assertVendorAuthorized(order_id, waId) {
     // async function ALWAYS returns Promise<boolean>
     const { data: order, error: orderErr } = await supabase
          .from("orders")
          .select("v_id")
          .eq("order_id", order_id)
          .single();

     if (orderErr || !order) return false;

     const { data: vendor, error: vendorErr } = await supabase
          .from("vendor_request")
          .select("mobile_number, status")
          .eq("v_id", order.v_id)
          .single();

     if (vendorErr || !vendor) return false;

     if (vendor.status === "blocked") return false;

     const vendorWa = vendor.mobile_number.replace(/\D/g, "");
     return vendorWa === waId;        // ← boolean
}

/**
 * ✅ Check: kya WhatsApp message expire ho chuka (e.g. 5 min se purana)?
 * @param {object} message - webhook ka message object
 * @param {number} minutes - allowed window
 * @returns {boolean}
 */
export function isMessageExpired(message, minutes = 5) {
     const tsMs = Number(message.timestamp || 0) * 1000;
     if (!tsMs) return true;
     const diff = Date.now() - tsMs;
     return diff > minutes * 60 * 1000;
}

export function calculateFinalAmount(order) {
     const items = order?.order_item || [];
     const vendor_discount = order?.vendor_discount || 0;

     const total_item_price = items.reduce(
          (sum, item) =>
               sum + (item?.item_real_price || 0) * (item?.quantity || 1),
          0
     );

     const discounted_amount =
          (total_item_price * (100 - vendor_discount)) / 100;
     const final_amount = discounted_amount || 0;

     return { total_item_price, vendor_discount, final_amount };
}
