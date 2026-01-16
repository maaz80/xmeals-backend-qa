// orderController.js
import fetch from "node-fetch";
import { assertVendorAuthorized, calculateFinalAmount, getFullOrderDetails } from "../../services/orderService.js";
import { supabase } from "../../config/supbase.js";

// ✅ Common helper: WhatsApp template send
export async function sendWhatsappTemplate({
     to,
     templateName,
     bodyParams = [],
     buttonPayload,
     order_id
}) {
     const url = `https://graph.facebook.com/v18.0/${process.env.WHATSAPP_PHONE_ID}/messages`;

     const jsonBody = {
          messaging_product: "whatsapp",
          to,
          type: "template",
          template: {
               name: templateName,
               language: { code: "en" },
               components: [
                    {
                         type: "body",
                         parameters: bodyParams,
                    },
               ],
          },
     };

     if (buttonPayload) {
          jsonBody.template.components.push({
               type: "button",
               sub_type: "quick_reply",
               index: "0",
               parameters: [
                    {
                         type: "payload",
                         payload: buttonPayload,
                    },
               ],
          });
     }

     const res = await fetch(url, {
          method: "POST",
          headers: {
               Authorization: `Bearer ${process.env.WHATSAPP_TOKEN}`,
               "Content-Type": "application/json",
          },
          body: JSON.stringify(jsonBody),
     });

     const json = await res.json();
     console.log("WhatsApp send response =>", json);
     // ✅ STORE MESSAGE ID IN ORDER
     if (json.messages?.[0]?.id && order_id) {
          await supabase
               .from("orders")
               .update({
                    wa_message_id: json.messages[0].id,
                    wa_message_created_ts: new Date()
               })
               .eq("order_id", order_id);
          console.log(`✅ Stored message ID ${json.messages[0].id} for order ${order_id}`);
     }
     return json;
}

// orderController.js me YE FUNCTION ADD KARO (sendWhatsappTemplate ke neeche)

export async function sendTextMessage({ to, text }) {
     const url = `https://graph.facebook.com/v18.0/${process.env.WHATSAPP_PHONE_ID}/messages`;

     const body = {
          messaging_product: "whatsapp",
          to,
          type: "text",
          text: { body: text },
     };

     const res = await fetch(url, {
          method: "POST",
          headers: {
               Authorization: `Bearer ${process.env.WHATSAPP_TOKEN}`,
               "Content-Type": "application/json",
          },
          body: JSON.stringify(body),
     });

     const json = await res.json();
     console.log("WhatsApp text message response =>", json);
     return json;
}

// 📌 Trigger → Backend: order create hone par vendor ko Accept template
export const onOrderCreated = async (req, res) => {
     try {
          const { order_id, v_id, user_order_id } = req.body;

          if (!order_id || !v_id) {
               return res.status(400).json({ error: "order_id or v_id missing" });
          }

          // 1️⃣ Order
          const { order, vendor, user, itemsText } = await getFullOrderDetails(order_id);

          const toNumber = vendor.mobile_number.replace(/\D/g, "");

          const { final_amount } = calculateFinalAmount(order);
          const displayOrderId = String(user_order_id || order.user_order_id || "");
          
          // Authorization check
          const allowed = await assertVendorAuthorized(order_id, toNumber);
          if (!allowed) {
               console.log("Unauthorized WhatsApp user for order", displayOrderId, toNumber);        
               return res.sendStatus(403);
          }
          
          // 6️⃣ Send "Accept Order" template
          const whatsappRes = await sendWhatsappTemplate({
               to: toNumber,
               templateName: "order_status", // <-- template name
               bodyParams: [
                    { type: "text", text: displayOrderId },   // {{1}} Order ID
                    { type: "text", text: String(final_amount ?? 0) }, // {{2}} Amount
                    { type: "text", text: user.name },                // {{3}} Customer
                    { type: "text", text: itemsText },                // {{4}} Items list
               ],
               buttonPayload: `ACCEPT_ORDER:${order_id}`,          // button_reply.id
               order_id,
          });

          return res.status(200).json({ message: "WhatsApp sent", whatsappRes });
     } catch (err) {
          console.error("onOrderCreated error:", err);
          return res.status(500).json({ error: err.message });
     }
};
