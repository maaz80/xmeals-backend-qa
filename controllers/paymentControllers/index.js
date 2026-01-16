import { razorpay } from "../../config/razorpay.js";
import { supabase } from "../../config/supbase.js";
import crypto from "crypto";
import jwt from 'jsonwebtoken';
import { decreaseWalletBalance } from "../walletControllers/walletController.js";
import { verifyPaymentWithRetry } from "../../services/verifyPaymentWithRetry.service.js";

export const initilisePayment = async (req, res) => {
  try {
    const { amount, currency = 'INR', orderPayload } = req.body;
    const { user } = req;

    if (!orderPayload || user?.id !== orderPayload?.p_user_id) {
      return res.status(403).json({ error: 'Unauthorized or missing payload.' });
    }

    // ✅ Pre-payment checks (cart total, min order)
    if (orderPayload?.p_item_total < 49) {
      return res.status(400).json({ error: 'Minimum order value ₹49 required.' });
    }

    if (orderPayload.p_wallet_used && orderPayload.p_wallet_used > 0) {
      try {
        const walletReq = {
          body: {
            amount: orderPayload.p_wallet_used,
            order_id: orderPayload.p_order_id,
            type: 'debit',
            description: 'Online order wallet deduction'
          },
          user
        };

        const walletPromise = new Promise((resolve, reject) => {
          const mockRes = {
            status: (code) => ({
              json: (data) => {
                if (code === 200) {
                  console.log('✅ Wallet balance decreased successfully:', data);
                  resolve(data);
                } else {
                  console.error('❌ Wallet decrease failed:', data);
                  reject(new Error(data.message || 'Wallet decrease failed'));
                }
              }
            })
          };

          // Call the wallet controller
          decreaseWalletBalance(walletReq, mockRes);
        });

        // Wait for wallet decrease to complete
        await walletPromise;

      } catch (walletError) {
        console.error('❌ Error decreasing wallet balance:', walletError);
        return res.status(400).json({
          message: 'Failed to deduct wallet balance',
          error: walletError.message
        });
      }
    }

    const receipt = `rcpt_${crypto.randomBytes(12).toString('hex')}`;

    const razorpayOrder = await razorpay.orders.create({
      amount: Math.round(amount * 100),
      currency,
      receipt
    });

    const enrichedOrderPayload = {
      ...orderPayload,
      p_razorpay_order_id: razorpayOrder.id
    };

    // ✅ Create pending order via RPC
    const { data: pendingOrder, error: rpcError } = await supabase.rpc("create_order", enrichedOrderPayload);

    if (rpcError) {
      console.error('❌ RPC error, order creation failed:', rpcError);
      return res.status(400).json({ error: rpcError.message });
    }

    // Safely handle array vs object
    const orderData = Array.isArray(pendingOrder) ? pendingOrder[0] : pendingOrder;

   
    if (!orderData) {
      return res.status(500).json({ message: 'Pending order creation failed.' });
    }

    switch (orderData.status) {
      case 'success':
        console.log('✅ Pending order created with ID:', orderData.order_id);
        break; // continue to Razorpay creation

      case 'item_not_found':
        return res.status(409).json({
          status: 'item_not_found',
          message: orderData.message || 'Some items are unavailable.',
          changed_items: orderData.v_unavailable_items
        });

      case 'item_deactivated':
        return res.status(409).json({
          status: 'item_deactivated',
          message: orderData.message || 'Some items had been deactivated.',
          changed_items: orderData.deactivated_items
        });

      case 'price_changed':
        return res.status(409).json({
          status: 'price_change',
          message: orderData.message || 'Prices have changed',
          changed_items: orderData.changed_items
        });

      case "vendor_not_found":
        return res.status(404).json({
          status: "vendor_not_found",
          message: orderData.message
        });

      // 🏪 VENDOR UNAVAILABLE
      case "vendor_unavailable":
        return res.status(409).json({
          status: "vendor_unavailable",
          message: orderData.message
        });

      // 📍 ADDRESS ISSUE
      case "address_not_found":
        return res.status(404).json({
          status: "address_not_found",
          message: orderData.message
        });

      // 💸 MIN ORDER FAIL
      case "min_order_fail":
        return res.status(400).json({
          status: "min_order_fail",
          message: orderData.message
        });

      // 🚫 COD NOT ALLOWED
      case "cod_unavailable":
        return res.status(409).json({
          status: "cod_unavailable",
          message: orderData.message
        });


      default:
        return res.status(500).json({ message: 'Unexpected response from RPC.' });
    }

    // ✅ Log success, continue to Razorpay order creation
    // console.log('✅ Pending order created with ID:', orderData.order_id);

    // ✅ update Razorpay payment order
    await razorpay.orders.edit(razorpayOrder.id, {
      notes: {
        internal_order_id: orderData.order_id
      }
    });


    const payloadHash = crypto.createHash('sha256', process.env.RAZORPAY_KEY_SECRET)
      .update(JSON.stringify(orderPayload))
      .digest('hex');

    const paymentToken = jwt.sign({
      hash: payloadHash,
      razorpay_order_id: razorpayOrder.id,
      user_id: user?.id
    }, process.env.JWT_SECRET, { expiresIn: '15m' });

    // ✅ Return Razorpay order + pending_order_id
    return res.status(200).json({
      id: razorpayOrder.id,
      amount: razorpayOrder.amount,
      currency: razorpayOrder.currency,
      token: paymentToken,
      pending_order_id: orderData.order_id
    });

  } catch (err) {
    console.error('🔥 Error initiating payment:', err);
    return res.status(500).json({ error: 'Failed to initiate order.' });
  }
};



export const finalisePayment = async (req, res) => {
  try {
    const {
      razorpay_order_id,
      razorpay_payment_id,
      razorpay_signature,
      orderPayload, // This contains all the params for your RPC function
      token,
      pending_order_id
    } = req.body;

    const { user } = req;
    const paymentType = orderPayload?.p_payment_type;
    let razorpayAmount = 0;

    // STEP A: VERIFY SIGNATURE (only for online payments)
    if (paymentType === 'online') {
      if (!razorpay_order_id || !razorpay_payment_id || !razorpay_signature) {
        return res.status(400).json({ message: 'Missing required payment parameters.' });
      }

      if (!token) {
        return res.status(401).json({ message: 'Authorization token is missing.' });
      }

      let decodedToken;
      try {
        decodedToken = jwt.verify(token, process.env.JWT_SECRET);
      } catch (err) {
        return res.status(401).json({ message: 'Invalid or expired payment token.' });
      }

      // console.log("decoded token", decodedToken);
      if (orderPayload?.p_user_id !== user?.id && decodedToken.user_id !== user?.id && orderPayload?.p_user_id !== decodedToken.user_id) {
        return res.status(403).json({ message: 'User is not authorized to finalize this payment.' });
      }

      if (decodedToken.razorpay_order_id !== razorpay_order_id) {
        return res.status(400).json({ message: 'Token and order ID mismatch.' });
      }

      const receivedPayloadHash = crypto.createHash('sha256', process.env.RAZORPAY_KEY_SECRET).update(JSON.stringify(orderPayload)).digest('hex');
      if (decodedToken.hash !== receivedPayloadHash) {
        return res.status(400).json({ message: 'Order details have been tampered with. Verification failed.' });
      }
      const body = razorpay_order_id + "|" + razorpay_payment_id;
      const expectedSignature = crypto
        .createHmac('sha256', process.env.RAZORPAY_KEY_SECRET)
        .update(body.toString())
        .digest('hex');

      if (expectedSignature !== razorpay_signature) {
        return res.status(400).json({ message: 'Invalid payment signature.' });
      }
      console.log('✅ Payment signature verified successfully.');

      const payment = await razorpay.payments.fetch(razorpay_payment_id);
      razorpayAmount = payment.amount;
      if (!payment) {
        return res.status(404).json({ message: "Payment not found on Razorpay." });
      }

      if (payment.order_id !== razorpay_order_id) {
        return res.status(400).json({ message: "Payment does not belong to the given order." });
      }

      if (payment.status !== "captured") {
        return res.status(400).json({ message: `Payment is not captured. Current status: ${payment.status}` });
      }


    }

    // STEP C: PREPARE AND CALL THE SECURE RPC FUNCTION
    const rpcParams = {
      p_order_id: pending_order_id,
      p_payment_type: orderPayload.p_payment_type,
      p_payment_id: razorpay_payment_id,
      p_razorpay_order_id: razorpay_order_id,
      p_paid_amount: razorpayAmount,
      p_user_id: orderPayload.p_user_id,
      p_address_id: orderPayload.p_address_id,
      p_cart_vendor_id: orderPayload.p_cart_vendor_id,
      p_cart_items: orderPayload.p_cart_items,
      p_tax_collected: orderPayload.p_tax_collected,
    };
    // Service call 
    const { data, error } = await verifyPaymentWithRetry({
      supabase,
      rpcParams
    });

    if (error) {
      const isTimeout =
        error.code === "57014" ||
        error.message?.includes("timeout");

      if (isTimeout) {
        return res.status(202).json({
          status: "processing_timeout"
        });
      }

      // 👇 tumhara existing error mapping
      return res.status(400).json({
        status: "failed",
        message: error.message
      });
    }

    const orderData = Array.isArray(data) ? data[0] : data;
    // STEP E: HANDLE BUSINESS LOGIC RESPONSES FROM THE FUNCTION
    if (orderData) {
      console.log("RPC Data from backend:", orderData);
      console.log('Order status changed for order ID:', pending_order_id);

      switch (orderData.status) {

        // ✅ SUCCESS
        case "success":
          return res.status(200).json({
            status: "success",
            order_id: orderData.order_id
          });

        // 🔁 ALREADY PROCESSED
        case "already_processed":
          return res.status(200).json({
            status: "already_processed",
            order_id: orderData.order_id
          });

        // 🔁 ALREADY FAILED (refund flow)
        case "already_failed":
          return res.status(409).json({
            status: "already_failed",
            message: orderData.message,
            order_id: orderData.order_id,
            refund_amount: orderData.refund_amount
          });

        // 🏪 VENDOR NOT FOUND
        case "vendor_not_found":
          return res.status(404).json({
            status: "vendor_not_found",
            message: orderData.message
          });

        // 🏪 VENDOR UNAVAILABLE
        case "vendor_unavailable":
          return res.status(409).json({
            status: "vendor_unavailable",
            message: orderData.message
          });

        // 📍 ADDRESS NOT FOUND
        case "address_not_found":
          return res.status(404).json({
            status: "address_not_found",
            message: orderData.message
          });

        // 🚫 ITEM NOT FOUND
        case "item_not_found":
          return res.status(409).json({
            status: "item_not_found",
            message: orderData.message,
            unavailable_items: orderData.unavailable_items
          });

        // 🚫 ITEM DEACTIVATED
        case "item_deactivated":
          return res.status(409).json({
            status: "item_deactivated",
            message: orderData.message,
            deactivated_items: orderData.deactivated_items
          });

        // 🔄 PRICE CHANGED
        case "price_changed":
          return res.status(409).json({
            status: "price_change",
            message: orderData.message,
            changed_items: orderData.changed_items
          });

        // 💸 MIN ORDER FAIL
        case "min_order_fail":
          return res.status(400).json({
            status: "min_order_fail",
            message: orderData.message
          });

        // 💳 PAYMENT FAILED
        case "payment_failed":
          return res.status(402).json({
            status: "payment_failed",
            reason: orderData.reason,
            order_id: orderData.order_id,
            expected_amount: orderData.expected_amount,
            paid_amount: orderData.paid_amount,
            difference_paise: orderData.difference_paise
          });

        // ❌ SAFETY NET
        default:
          console.error("❌ Unknown verify_payment RPC response:", orderData);
          return res.status(500).json({
            status: "unknown_error",
            message: "Unexpected response from payment verification."
          });
      }

    }

    // STEP 3: Fallback if no data and no error
    return res.status(500).json({ message: 'Unexpected state: no RPC data and no error.' });

  } catch (err) {
    console.error('🔥 Fatal Error in /api/finalize-order:', err);
    res.status(500).json({ message: 'Internal server error during order finalization.' });
  }
}

export const codOrderCreation = async (req, res) => {
  try {
    const {
      orderPayload
    } = req.body;

    const { user } = req;

    if (orderPayload?.p_user_id !== user?.id) {
      return res.status(403).json({ message: 'User is not authorized to create order.' });
    }

    // STEP B: DECREASE WALLET BALANCE (if wallet is used)
    if (orderPayload.p_wallet_used && orderPayload.p_wallet_used > 0) {
      try {
        // Create a mock request and response object for the wallet controller
        const walletReq = {
          body: {
            amount: orderPayload.p_wallet_used,
            order_id: orderPayload.p_order_id,
            type: 'debit',
            description: 'Order payment deduction'
          },
          user: user
        };

        // Create a promise to handle the wallet decrease
        const walletPromise = new Promise((resolve, reject) => {
          const mockRes = {
            status: (code) => ({
              json: (data) => {
                if (code === 200) {
                  console.log('✅ Wallet balance decreased successfully:', data);
                  resolve(data);
                } else {
                  console.error('❌ Wallet decrease failed:', data);
                  reject(new Error(data.message || 'Wallet decrease failed'));
                }
              }
            })
          };

          // Call the wallet controller
          decreaseWalletBalance(walletReq, mockRes);
        });

        // Wait for wallet decrease to complete
        await walletPromise;

      } catch (walletError) {
        console.error('❌ Error decreasing wallet balance:', walletError);
        return res.status(400).json({
          message: 'Failed to deduct wallet balance',
          error: walletError.message
        });
      }
    }

    // STEP C: PREPARE AND CALL THE SECURE RPC FUNCTION
    const rpcParams = {
      ...orderPayload,
      p_razorpay_order_id: "cod",
    };

    // Assuming you have a Supabase service role client initialized
    // const supabaseAdmin = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
    const { data, error } = await supabase.rpc("create_order", rpcParams);

    // STEP D: HANDLE POSTGRES-LEVEL ERRORS (e.g., connection issue, RLS violation)
    if (error) {
      console.error("❌ Supabase RPC Error:", error);
      console.error('Order creation failed for order ID:', orderPayload?.p_order_id);
      // Default fallback
      let statusCode = 400;
      let message = error.message || "Order creation failed";

      // If message is JSON, parse it
      if (typeof error.message === "string" && error.message.trim().startsWith("{")) {
        try {
          const parsed = JSON.parse(error.message);
          statusCode = parsed.status || statusCode;
          return res.status(statusCode).json(parsed);
        } catch (_) {
          return res.status(500).json({ message: "Failed to place order after payment." });
        }
      }

      // Plain postgres exception
      return res.status(statusCode).json({
        status: statusCode,
        message
      });
    }
    const orderData = Array.isArray(data) ? data[0] : data;

    // STEP E: HANDLE BUSINESS LOGIC RESPONSES FROM THE FUNCTION
    if (!orderData) {
      return res.status(500).json({ message: 'Order creation failed.' });
    }

    switch (orderData.status) {
      case 'success':
        console.log('✅ Order created with ID:', orderData.order_id);
        return res.status(200).json({
          status: "success",
          order_id: orderData.order_id
        });

      case 'item_not_found':
        return res.status(409).json({
          status: 'item_not_found',
          message: orderData.message || 'Some items are unavailable.',
          changed_items: orderData.v_unavailable_items
        });

      case 'item_deactivated':
        return res.status(409).json({
          status: 'item_deactivated',
          message: orderData.message || 'Some items had been deactivated.',
          changed_items: orderData.deactivated_items
        });

      case 'price_changed':
        return res.status(409).json({
          status: 'price_change',
          message: orderData.message || 'Prices have changed',
          changed_items: orderData.changed_items
        });

      case "vendor_not_found":
        return res.status(404).json({
          status: "vendor_not_found",
          message: orderData.message
        });

      // 🏪 VENDOR UNAVAILABLE
      case "vendor_unavailable":
        return res.status(409).json({
          status: "vendor_unavailable",
          message: orderData.message
        });

      // 📍 ADDRESS ISSUE
      case "address_not_found":
        return res.status(404).json({
          status: "address_not_found",
          message: orderData.message
        });

      // 💸 MIN ORDER FAIL
      case "min_order_fail":
        return res.status(400).json({
          status: "min_order_fail",
          message: orderData.message
        });

      // 🚫 COD NOT ALLOWED
      case "cod_unavailable":
        return res.status(409).json({
          status: "cod_unavailable",
          message: orderData.message
        });


      default:
        return res.status(500).json({ message: 'Unexpected response from RPC.' });
    }

  } catch (err) {
    console.error('🔥 Fatal Error in /api/cod-order-creation:', err);
    res.status(500).json({ message: 'Internal server error during order finalization.' });
  }
}

