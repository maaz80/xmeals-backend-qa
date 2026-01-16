import { supabase } from "../../config/supbase.js";

export const updateWalletBalance = async (req, res) => {
  try {
    const {
      amount,
      order_id,
      type,
      description,
      referral,
      referralCode,
      cancel = false,
      review = false,
      signup = false,
    } = req.body;

    const { user } = req;

    if(!referral && !cancel && !review && !signup){
      return res.status(400).json({ message: "Invalid request" });
    }

    if (!user) return res.status(401).json({ message: "Unauthorized" });

    if (!amount || typeof amount !== "number") {
      return res.status(400).json({ message: "Invalid amount" });
    }

    let userId;
    if (referral && referralCode) {
      // Handle referral code logic here
      const { data: userData, error: userError } = await supabase
        .from("user")
        .select("referred_by")
        .eq("user_id", user.id)
        .single();

      if (userError || !userData) {
        return res.status(400).json({ message: "User not found" });
      }



      if (userData.referred_by && userData.referred_by === referralCode) {
        //check if referral code is belongs to same user
        const { data, error } = await supabase
          .from("user")
          .select("user_id,wallet_balance")
          .eq("referral_code", referralCode)
          .single();

        console.log("data", data);
        if (error || !data) {
          return res.status(400).json({ message: "Invalid referral code" });
        }

        if (data.user_id === user.id) {
          return res
            .status(400)
            .json({ message: "You cannot use your own referral code" });
        }

        userId = data.user_id;
      }
    }

    // if (cancel) {
    //   //check if order is already cancel or not
    //   const { data: existingOrder, error: fetchError } = await supabase
    //     .from("orders")
    //     .select("status")
    //     .eq("order_id", order_id)
    //     .single();

    //   if (fetchError) {
    //     throw fetchError;
    //   }

    //   if (existingOrder?.status === "cancelled") {
    //     return res.status(400).json({ message: "Order is already cancelled" });
    //   }
    // }

    if (review) {
      const { data, error } = await supabase
        .from("user_reviews")
        .select("id")
        .eq("user_id", user.id);
      if (error)
        return res.status(400).json({ message: "Error fetching user reviews" });
      if (data.length < 1)
        return res
          .status(400)
          .json({ message: "User has not reviewed the app" });

      if (data.length > 1) {
        return res
          .status(400)
          .json({ message: "User has already reviewed the app" });
      }
    }

    if(signup){
      // Check if user has already received signup bonus
      const { data: transactionData, error: transactionError } = await supabase
        .from("wallet_transactions")
        .select("id")
        .eq("user_id", user.id)
        .eq("description", "Signup bonus added");

      if (transactionError) {
        throw transactionError;
      }

      if (transactionData.length > 0) {
        return res
          .status(400)
          .json({ message: "User has already received signup bonus" });
      }
    }
    // Call Postgres function
    const { data, error } = await supabase.rpc("increase_wallet_balance", {
      p_user_id: referral ? userId : user.id,
      p_amount: amount,
      p_transaction_type: type || "credit",
      p_order_id: order_id || null,
      p_description: description || null,
    });

    if (error) {
      console.error("Supabase error:", error);
      return res.status(400).json({ message: error.message });
    }

    return res.status(200).json({
      message: "Wallet updated successfully",
      wallet: data.wallet,
    });
  } catch (err) {
    console.error("Server error:", err);
    return res.status(500).json({ message: "Internal Server Error" });
  }
};

export const decreaseWalletBalance = async (req, res) => {
  try {
    const { amount, order_id, type, description } = req.body;
    const { user } = req;

    if (!amount || typeof amount !== "number") {
      return res.status(400).json({ message: "Invalid amount" });
    }

    // Call Postgres function
    const { data, error } = await supabase.rpc("decrease_wallet_balance", {
      p_user_id: user.id,
      p_amount: amount,
      p_transaction_type: type || "debit",
      p_order_id: order_id || null,
      p_description: description || null,
    });

    if (error) {
      console.error("Supabase error:", error);
      return res.status(400).json({ message: error.message });
    }

    console.log("Data from decrease_wallet_balance:", data);
    return res.status(200).json({
      message: "Wallet updated successfully",
      wallet: data.wallet,
    });
  } catch (err) {
    console.error("Server error:", err);
    return res.status(500).json({ message: "Internal Server Error" });
  }
};
