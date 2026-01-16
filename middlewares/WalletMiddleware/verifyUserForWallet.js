import { supabase } from "../../config/supbase.js";

export const verifyUserForWallet = async (req, res, next) => {
  // add try catch block
  try {
    const authHeader = req.headers["authorization"];
    if (!authHeader) return res.status(401).json({ error: "No token" });

    const token = authHeader.split(" ")[1];
    const { data, error } = await supabase.auth.getUser(token);

    if (error || !data?.user) {
      return res.status(401).json({ error: "Invalid token" });
    }

    //if user exist and user is not blocked then only place user in req.user and also check if user_id in payload and in here are same
    const { data: userData, error: userError } = await supabase
      .from("user")
      .select("blocked, role")
      .eq("user_id", data.user.id)
      .single();


    if (userError || !userData) {
      return res.status(401).json({ error: "User not found or blocked" });
    }

    if (userData.blocked) {
      return res.status(403).json({ error: "User is blocked" });
    }
//changee: user agr admin hue to uska alg check krna h 
// changee: agr cancel ka request user admin ni h to exception trhow krna h 
    if(userData?.role === "Admin" && req.body.cancel) {
      req.user = { id: req.body.user_id };
    }else if(userData?.role !== "Admin" && req.body.cancel) {
      return res.status(403).json({ error: "User is not authorized to cancel this order." });
    }else{
      req.user = data.user;
    }
    next();
  } catch (error) {
    console.error("Error in verifyUser middleware:", error);
    return res.status(500).json({ error: "Internal server error" });
  }
};
