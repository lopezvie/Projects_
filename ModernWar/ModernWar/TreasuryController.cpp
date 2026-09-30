#include "TreasuryController.h"


namespace ModernWarCore
{
	std::string SpendTreasury(WorldState& world, std::uint32_t nationId, std::int64_t amount)
	{
		std::string result = ModernWarCore::ValidateRoster(world.Nations);
		if (!result.empty())
		{
			return result;
		}
		else
		{
			NationState* nation = FindNation(world, nationId);
			if (nation != nullptr)
			{
				if (amount <= 0)
					return "Amount must be > 0";
				else if (amount > nation->Treasury)
				{
					return "Insufficient treasury";
				}
				else
				{
					nation->Treasury -= amount;
					return "";
				}
			}
			else
				return "NationId not found";
		}

	}

	std::string CreditTreasury(WorldState& world, std::uint32_t nationId, std::int64_t amount)
	{
		std::string result = ModernWarCore::ValidateRoster(world.Nations);
		if (!result.empty())
		{
			return result;
		}
		else
		{
			NationState* nation = FindNation(world, nationId);
			if (nation != nullptr)
			{
				if (amount <= 0)
					return "Amount must be > 0";
				else if (nation->Treasury > std::numeric_limits<std::int64_t>::max() - amount)
				{
					return "Treasury limit exceeded";
				}
				else
				{
					nation->Treasury += amount;
					return "";
				}
			}
			else
				return "NationId not found";
		}
	}

	std::string TransferTreasury(WorldState& world, std::uint32_t sourceNationId, std::uint32_t destinationNationId, std::int64_t amount)
	{
		std::string result = ModernWarCore::ValidateRoster(world.Nations);
		if (result.empty())
		{
			NationState* source_nation = FindNation(world, sourceNationId);
			if (source_nation != nullptr)
			{
				NationState* destination_nation = FindNation(world, destinationNationId);
				if (destination_nation != nullptr)
				{
					if (sourceNationId != destinationNationId)
					{
						WorldState world_copy = { world.TurnNumber, world.Nations };
						std::string spending_result = ModernWarCore::SpendTreasury(world_copy, sourceNationId, amount);
						if (spending_result.empty())
						{
							std::string crediting_result = ModernWarCore::CreditTreasury(world_copy, destinationNationId, amount);
							if (crediting_result.empty())
							{
								world.TurnNumber = world_copy.TurnNumber;
								world.Nations = world_copy.Nations;
								return "";
							}
							else
							{
								return crediting_result;
							}
						}
						else
						{
							return spending_result;
						}
					}
					else
						return "Source and destination must differ";
				}
				else
					return "Destination NationId not found";
			}
			else
				return "Source NationId not found";
		}
		else
			return result;
	}

	NationState* FindNation(WorldState& world, std::uint32_t nationId)
	{
		auto it = std::find_if(world.Nations.begin(), world.Nations.end(), [nationId](NationState nation) {return nation.NationId == nationId; });
		if (it == world.Nations.end())
		{
			return nullptr;
		}
		else
			return &(*it);
	}

	const NationState* FindNation(const WorldState& world, std::uint32_t nationId)
	{
		auto it = std::find_if(world.Nations.begin(), world.Nations.end(), [nationId](NationState nation) {return nation.NationId == nationId; });
		if (it == world.Nations.end())
		{
			return nullptr;
		}
		else
			return &(*it);
	}
}