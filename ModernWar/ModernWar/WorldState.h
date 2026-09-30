#pragma once
#include <cstdint>
#include <array>
#include "NationState.h"


namespace ModernWarCore
{
	struct WorldState
	{
		std::uint32_t TurnNumber = 1;
		std::array<ModernWarCore::NationState, 2> Nations;
	};
}